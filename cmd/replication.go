package cmd

import (
	"context"
	"fmt"
	"github.com/zrepl/zrepl/cmd/replication"
	"github.com/zrepl/zrepl/rpc"
	"github.com/zrepl/zrepl/zfs"
	"io"
)

type localPullACL struct{}

func (a localPullACL) Filter(p *zfs.DatasetPath) (pass bool, err error) {
	return true, nil
}

type InitialReplPolicy string

const (
	InitialReplPolicyMostRecent InitialReplPolicy = "most_recent"
	InitialReplPolicyAll        InitialReplPolicy = "all"
)

const DEFAULT_INITIAL_REPL_POLICY = InitialReplPolicyMostRecent

type Puller struct {
	task                    *Task
	Remote                  rpc.RPCClient
	Mapping                 *DatasetMapFilter
	InitialReplPolicy       InitialReplPolicy
	FilesystemVersionFilter zfs.FilesystemVersionFilter
}

func (p *Puller) ListFilesystems() ([]replication.Filesystem, error) {
	f, err := p.Mapping.InvertedFilter()
	if err != nil {
		return nil, err
	}
	ch := make(chan zfs.ZFSListResult)
	props := []string{"name", "receive_resume_token"}
	go zfs.ZFSListChan(context.Background(), ch, props)
	defer close(ch)
	fss := make([]replication.Filesystem, 0)
	for res := range ch {
		if res.Err != nil {
			return nil, res.Err
		}
		token := res.Fields[1]
		if token == "-" {
			token = ""
		}
		fss = append(fss, replication.Filesystem{
			Path:        res.Fields[0],
			ResumeToken: res.Fields[1],
		})
	}
	return fss, nil
}

func (p *Puller) ListFilesystemVersions(fs string) ([]zfs.FilesystemVersion, error) {
	dp, err := zfs.NewDatasetPath(fs)
	if err != nil {
		return nil, err
	}
	f, err := p.Mapping.InvertedFilter()
	if err != nil {
		return nil, err
	}
	pass, err := f.Filter(dp)
	if err != nil {
		return nil, err
	}
	if !pass {
		return nil, replication.NewFilteredError(fs)
	}
	return zfs.ZFSListFilesystemVersions(dp, p.FilesystemVersionFilter)

}

func (p *Puller) Send(r replication.SendRequest) (replication.SendResponse, error) {
	return replication.SendResponse{}, fmt.Errorf("puller does not send")
}

func (p *Puller) Receive(r replication.ReceiveRequest) (io.Writer, error) {
	dp, err := zfs.NewDatasetPath(r.Filesystem)
	if err != nil {
		return nil, err
	}
	f, err := p.Mapping.InvertedFilter()
	if err != nil {
		return nil, err
	}
	pass, err := f.Filter(dp)
	if err != nil {
		return nil, err
	}
	if !pass {
		return nil, replication.NewFilteredError(r.Filesystem)
	}
	if r.ResumeToken != "" {
		localToken, err := zfs.ZFSGetReceiveResumeToken(dp)
		if err != nil {
			return nil, err
		}
		if localToken != r.ResumeToken {
			return nil, fmt.Errorf("receive-side resume token does not metch send-side resume token")
		}
	}
	writer, err := zfs.ZFSRecvWriter(dp)
	if err != nil {
		return nil, err
	}
	// FIXME close writer
	return writer, nil
}
