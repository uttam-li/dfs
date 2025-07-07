package client

// import (
// 	"log"
// 	"time"

// 	"github.com/hanwen/go-fuse/v2/fuse"
// )

// const (
// 	F_RDLCK = 0
// 	F_WRLCK = 1
// 	F_UNLCK = 2
// )

// func (fs *Filesystem) GetLk(cancel <-chan struct{}, input *fuse.LkIn, out *fuse.LkOut) (code fuse.Status) {
// 	start := time.Now()

// 	if fs.config.Debug {
// 		defer func() {
// 			log.Printf("[DEBUG] GetLk: inode=%d fh=%d duration=%v",
// 				input.NodeId, input.Fh, time.Since(start))
// 		}()
// 	}

// 	// GFS doesn't implement POSIX file locking
// 	// Return that no lock conflicts exist
// 	out.Lk.Start = input.Lk.Start
// 	out.Lk.Len = input.Lk.Len
// 	out.Lk.Typ = F_UNLCK // No lock
// 	out.Lk.Pid = 0

// 	return fuse.OK
// }

// func (fs *Filesystem) SetLk(cancel <-chan struct{}, input *fuse.LkIn) (code fuse.Status) {
// 	start := time.Now()

// 	if fs.config.Debug {
// 		defer func() {
// 			log.Printf("[DEBUG] SetLk: inode=%d fh=%d start=%d len=%d type=%d duration=%v",
// 				input.NodeId, input.Fh, input.Lk.Start, input.Lk.Len, input.Lk.Typ, time.Since(start))
// 		}()
// 	}

// 	// GFS doesn't implement POSIX file locking
// 	// For consistency with NFS behavior, we allow locks but don't enforce them
// 	// This prevents applications from failing while providing no actual locking

// 	if input.Lk.Typ == F_UNLCK {
// 		// Unlock operation - always succeeds
// 		return fuse.OK
// 	}

// 	// Lock operation - simulate success
// 	// In a real implementation with locking, this would:
// 	// 1. Validate the lock request
// 	// 2. Check for conflicts with existing locks
// 	// 3. Store the lock in a lock table
// 	// 4. Return appropriate errors for conflicts

// 	return fuse.OK
// }

// func (fs *Filesystem) SetLkw(cancel <-chan struct{}, input *fuse.LkIn) (code fuse.Status) {
// 	start := time.Now()

// 	if fs.config.Debug {
// 		defer func() {
// 			log.Printf("[DEBUG] SetLkw: inode=%d fh=%d start=%d len=%d type=%d duration=%v",
// 				input.NodeId, input.Fh, input.Lk.Start, input.Lk.Len, input.Lk.Typ, time.Since(start))
// 		}()
// 	}

// 	// SetLkw is the blocking version of SetLk
// 	// Since we don't implement real locking, treat it the same as SetLk
// 	return fs.SetLk(cancel, input)
// }
