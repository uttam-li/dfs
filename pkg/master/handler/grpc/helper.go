package grpc

import (
	"github.com/uttam-li/dfs/api/generated/common"
	"github.com/uttam-li/dfs/api/generated/master"
	commonTypes "github.com/uttam-li/dfs/pkg/common"
	"github.com/uttam-li/dfs/pkg/master/types"
)

func convertInoForGrpc(inode *types.Inode) *master.Inode {
	return &master.Inode{
		Ino: inode.Ino,
		Attributes: &common.FileAttributes{
			Mode:  inode.GetMode(),
			Gid:   inode.GetGID(),
			Uid:   inode.GetUID(),
			Size:  inode.Size,
			Atime: int64(inode.Mtime),
			Mtime: int64(inode.Mtime),
			Ctime: int64(inode.Mtime),
			Nlink: uint32(inode.Nlink),
		},
		Xattrs:       inode.Xattr,
		ChunkHandles: convertChunks(inode.Chunks),
	}
}

func convertChunks(chunks map[uint64]commonTypes.ChunkHandle) map[uint64]string {
	if chunks == nil {
		return nil
	}

	maxIndex := uint64(0)
	for index := range chunks {
		if index > maxIndex {
			maxIndex = index
		}
	}

	results := make(map[uint64]string, len(chunks)+1)

	for index, handle := range chunks {
		results[index] = handle.String()
	}

	return results
}

func convertInodeToStatxAttrs(inode *types.Inode) *master.StatxAttributes {
	// Get timestamps
	mtime := int64(inode.Mtime)

	return &master.StatxAttributes{
		Mode:      inode.GetMode(),
		Uid:       inode.GetUID(),
		Gid:       inode.GetGID(),
		Size:      inode.Size,
		Blocks:    (inode.Size + 511) / 512, // Number of 512-byte blocks
		Blksize:   4096,                     // Preferred I/O block size
		Nlink:     uint32(inode.Nlink),
		Ino:       inode.Ino,
		RdevMajor: 0,
		RdevMinor: 0,
		DevMajor:  0,
		DevMinor:  0, 
		Atime: &master.StatxTimestamp{
			Sec:  mtime, // Use mtime as atime for now
			Nsec: 0,
		},
		Mtime: &master.StatxTimestamp{
			Sec:  mtime,
			Nsec: 0,
		},
		Ctime: &master.StatxTimestamp{
			Sec:  mtime, // Use mtime as ctime for now
			Nsec: 0,
		},
		Btime: &master.StatxTimestamp{
			Sec:  mtime, // Use mtime as birth time for now
			Nsec: 0,
		},
	}
}
