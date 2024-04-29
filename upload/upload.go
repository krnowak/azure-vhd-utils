package upload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Microsoft/azure-vhd-utils/upload/concurrent"
	"github.com/Microsoft/azure-vhd-utils/upload/progress"
	"github.com/Microsoft/azure-vhd-utils/vhdcore/common"
	"github.com/Microsoft/azure-vhd-utils/vhdcore/diskstream"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
)

// DiskUploadContext type describes VHD upload context, this includes the disk stream to read from, the ranges of the
// stream to read, the client representing the destination blob in its container and used to communicate with Azure
// storage and the number of parallel go-routines to use for upload.
type DiskUploadContext struct {
	VhdStream             *diskstream.DiskStream // The stream whose ranges needs to be uploaded
	AlreadyProcessedBytes int64                  // The size in bytes already uploaded
	UploadableRanges      []*common.IndexRange   // The subset of stream ranges to be uploaded
	PageblobClient        *pageblob.Client       // The client to make Azure blob service API calls
	Parallelism           int                    // The number of concurrent goroutines to be used for upload
	Resume                bool                   // Indicate whether this is a new or resuming upload
}

// oneMB is one MegaByte
const oneMB = float64(1048576)

type byteReadSeekCloser struct {
	*bytes.Reader
}

func newByteReadSeekCloser(b []byte) io.ReadSeekCloser {
	return byteReadSeekCloser{bytes.NewReader(b)}
}

func (byteReadSeekCloser) Close() error {
	return nil
}

var _ io.ReadSeekCloser = byteReadSeekCloser{}

// Upload uploads the disk ranges described by the parameter cxt, this parameter describes the disk stream to
// read from, the ranges of the stream to read, the destination blob and it's container, the client to communicate
// with Azure storage and the number of parallel go-routines to use for upload.
func Upload(cxt *DiskUploadContext) error {
	// Get the channel that contains stream of disk data to upload
	dataWithRangeChan, streamReadErrChan := GetDataWithRanges(cxt.VhdStream, cxt.UploadableRanges)

	// The channel to send upload request to load-balancer
	requtestChan := make(chan *concurrent.Request, 0)

	// Prepare and start the load-balancer that load request across 'cxt.Parallelism' workers
	loadBalancer := concurrent.NewBalancer(cxt.Parallelism)
	loadBalancer.Init()
	workerErrorChan, allWorkersFinishedChan := loadBalancer.Run(requtestChan)

	// Calculate the actual size of the data to upload
	uploadSizeInBytes := int64(0)
	for _, r := range cxt.UploadableRanges {
		uploadSizeInBytes += r.Length()
	}
	fmt.Printf("\nEffective upload size: %.2f MB (from %.2f MB originally)", float64(uploadSizeInBytes)/oneMB, float64(cxt.VhdStream.GetSize())/oneMB)

	// Prepare and start the upload progress tracker
	uploadProgress := progress.NewStatus(cxt.Parallelism, cxt.AlreadyProcessedBytes, uploadSizeInBytes, progress.NewComputestateDefaultSize())
	progressChan := uploadProgress.Run()

	// read progress status from progress tracker and print it
	go readAndPrintProgress(progressChan, cxt.Resume)

	// listen for errors reported by workers and print it
	var allWorkSucceeded = true
	go func() {
		for {
			fmt.Println(<-workerErrorChan)
			allWorkSucceeded = false
		}
	}()

	var err error
L:
	for {
		select {
		case dataWithRange, ok := <-dataWithRangeChan:
			if !ok {
				close(requtestChan)
				break L
			}

			// Create work request
			//
			req := &concurrent.Request{
				Work: func() error {
					_, err := cxt.PageblobClient.UploadPages(context.TODO(),
						newByteReadSeekCloser(dataWithRange.Data),
						blob.HTTPRange{
							Offset: dataWithRange.Range.Start,
							Count:  dataWithRange.Range.Length(),
						},
						nil)
					if err == nil {
						uploadProgress.ReportBytesProcessedCount(dataWithRange.Range.Length())
					}
					return err
				},
				ShouldRetry: func(e error) bool {
					return true
				},
				ID: dataWithRange.Range.String(),
			}

			// Send work request to load balancer for processing
			//
			requtestChan <- req
		case err = <-streamReadErrChan:
			close(requtestChan)
			loadBalancer.TearDownWorkers()
			break L
		}
	}

	<-allWorkersFinishedChan
	uploadProgress.Close()

	if !allWorkSucceeded {
		err = errors.New("\nUpload Incomplete: Some blocks of the VHD failed to upload, rerun the command to upload those blocks")
	}

	if err == nil {
		fmt.Printf("\r Completed: %3d%% [%10.2f MB] RemainingTime: %02dh:%02dm:%02ds Throughput: %d Mb/sec  %2c ",
			100,
			float64(uploadSizeInBytes)/oneMB,
			0, 0, 0,
			0, ' ')

	}
	return err
}

// GetDataWithRanges with start reading and streaming the ranges from the disk identified by the parameter ranges.
// It returns two channels, a data channel to stream the disk ranges and a channel to send any error while reading
// the disk. On successful completion the data channel will be closed. the caller must not expect any more value in
// the data channel if the error channel is signaled.
func GetDataWithRanges(stream *diskstream.DiskStream, ranges []*common.IndexRange) (<-chan *DataWithRange, <-chan error) {
	dataWithRangeChan := make(chan *DataWithRange, 0)
	errorChan := make(chan error, 0)
	go func() {
		for _, r := range ranges {
			dataWithRange := &DataWithRange{
				Range: r,
				Data:  make([]byte, r.Length()),
			}
			_, err := stream.Seek(r.Start, 0)
			if err != nil {
				errorChan <- err
				return
			}
			_, err = io.ReadFull(stream, dataWithRange.Data)
			if err != nil {
				errorChan <- err
				return
			}
			dataWithRangeChan <- dataWithRange
		}
		close(dataWithRangeChan)
	}()
	return dataWithRangeChan, errorChan
}

// readAndPrintProgress reads the progress records from the given progress channel and output it. It reads the
// progress record until the channel is closed.
func readAndPrintProgress(progressChan <-chan *progress.Record, resume bool) {
	var spinChars = [4]rune{'\\', '|', '/', '-'}
	s := time.Time{}
	if resume {
		fmt.Println("\nResuming VHD upload..")
	} else {
		fmt.Println("\nUploading the VHD..")
	}

	i := 0
	for progressRecord := range progressChan {
		if i == 4 {
			i = 0
		}
		t := s.Add(progressRecord.RemainingDuration)
		fmt.Printf("\r Completed: %3d%% [%10.2f MB] RemainingTime: %02dh:%02dm:%02ds Throughput: %d Mb/sec  %2c ",
			int(progressRecord.PercentComplete),
			float64(progressRecord.BytesProcessed)/oneMB,
			t.Hour(), t.Minute(), t.Second(),
			int(progressRecord.AverageThroughputMbPerSecond),
			spinChars[i],
		)
		i++
	}
}
