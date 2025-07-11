// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.5
// 	protoc        v3.19.6
// source: api/proto/common.proto

package common

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// File attributes used across services
type FileAttributes struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Mode          uint32                 `protobuf:"varint,1,opt,name=mode,proto3" json:"mode,omitempty"`
	Uid           uint32                 `protobuf:"varint,2,opt,name=uid,proto3" json:"uid,omitempty"`
	Gid           uint32                 `protobuf:"varint,3,opt,name=gid,proto3" json:"gid,omitempty"`
	Size          uint64                 `protobuf:"varint,4,opt,name=size,proto3" json:"size,omitempty"`
	Atime         int64                  `protobuf:"varint,5,opt,name=atime,proto3" json:"atime,omitempty"`
	Mtime         int64                  `protobuf:"varint,6,opt,name=mtime,proto3" json:"mtime,omitempty"`
	Ctime         int64                  `protobuf:"varint,7,opt,name=ctime,proto3" json:"ctime,omitempty"`
	Nlink         uint32                 `protobuf:"varint,8,opt,name=nlink,proto3" json:"nlink,omitempty"`
	Blocks        uint64                 `protobuf:"varint,9,opt,name=blocks,proto3" json:"blocks,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FileAttributes) Reset() {
	*x = FileAttributes{}
	mi := &file_api_proto_common_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FileAttributes) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FileAttributes) ProtoMessage() {}

func (x *FileAttributes) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_common_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FileAttributes.ProtoReflect.Descriptor instead.
func (*FileAttributes) Descriptor() ([]byte, []int) {
	return file_api_proto_common_proto_rawDescGZIP(), []int{0}
}

func (x *FileAttributes) GetMode() uint32 {
	if x != nil {
		return x.Mode
	}
	return 0
}

func (x *FileAttributes) GetUid() uint32 {
	if x != nil {
		return x.Uid
	}
	return 0
}

func (x *FileAttributes) GetGid() uint32 {
	if x != nil {
		return x.Gid
	}
	return 0
}

func (x *FileAttributes) GetSize() uint64 {
	if x != nil {
		return x.Size
	}
	return 0
}

func (x *FileAttributes) GetAtime() int64 {
	if x != nil {
		return x.Atime
	}
	return 0
}

func (x *FileAttributes) GetMtime() int64 {
	if x != nil {
		return x.Mtime
	}
	return 0
}

func (x *FileAttributes) GetCtime() int64 {
	if x != nil {
		return x.Ctime
	}
	return 0
}

func (x *FileAttributes) GetNlink() uint32 {
	if x != nil {
		return x.Nlink
	}
	return 0
}

func (x *FileAttributes) GetBlocks() uint64 {
	if x != nil {
		return x.Blocks
	}
	return 0
}

// Chunk location information
type ChunkLocation struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	ServerId      string                 `protobuf:"bytes,1,opt,name=server_id,json=serverId,proto3" json:"server_id,omitempty"`
	Address       string                 `protobuf:"bytes,2,opt,name=address,proto3" json:"address,omitempty"`
	Version       uint32                 `protobuf:"varint,3,opt,name=version,proto3" json:"version,omitempty"`
	IsPrimary     bool                   `protobuf:"varint,4,opt,name=is_primary,json=isPrimary,proto3" json:"is_primary,omitempty"`
	LeaseExpiry   int64                  `protobuf:"varint,5,opt,name=lease_expiry,json=leaseExpiry,proto3" json:"lease_expiry,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ChunkLocation) Reset() {
	*x = ChunkLocation{}
	mi := &file_api_proto_common_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ChunkLocation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChunkLocation) ProtoMessage() {}

func (x *ChunkLocation) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_common_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ChunkLocation.ProtoReflect.Descriptor instead.
func (*ChunkLocation) Descriptor() ([]byte, []int) {
	return file_api_proto_common_proto_rawDescGZIP(), []int{1}
}

func (x *ChunkLocation) GetServerId() string {
	if x != nil {
		return x.ServerId
	}
	return ""
}

func (x *ChunkLocation) GetAddress() string {
	if x != nil {
		return x.Address
	}
	return ""
}

func (x *ChunkLocation) GetVersion() uint32 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *ChunkLocation) GetIsPrimary() bool {
	if x != nil {
		return x.IsPrimary
	}
	return false
}

func (x *ChunkLocation) GetLeaseExpiry() int64 {
	if x != nil {
		return x.LeaseExpiry
	}
	return 0
}

var File_api_proto_common_proto protoreflect.FileDescriptor

var file_api_proto_common_proto_rawDesc = string([]byte{
	0x0a, 0x16, 0x61, 0x70, 0x69, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6d, 0x6d,
	0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x03, 0x64, 0x66, 0x73, 0x22, 0xcc, 0x01,
	0x0a, 0x0e, 0x46, 0x69, 0x6c, 0x65, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73,
	0x12, 0x12, 0x0a, 0x04, 0x6d, 0x6f, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x04,
	0x6d, 0x6f, 0x64, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0d, 0x52, 0x03, 0x75, 0x69, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x67, 0x69, 0x64, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x0d, 0x52, 0x03, 0x67, 0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x73, 0x69, 0x7a, 0x65,
	0x18, 0x04, 0x20, 0x01, 0x28, 0x04, 0x52, 0x04, 0x73, 0x69, 0x7a, 0x65, 0x12, 0x14, 0x0a, 0x05,
	0x61, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x61, 0x74, 0x69,
	0x6d, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x6d, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x05, 0x6d, 0x74, 0x69, 0x6d, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x63, 0x74, 0x69, 0x6d,
	0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0x12, 0x14,
	0x0a, 0x05, 0x6e, 0x6c, 0x69, 0x6e, 0x6b, 0x18, 0x08, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x05, 0x6e,
	0x6c, 0x69, 0x6e, 0x6b, 0x12, 0x16, 0x0a, 0x06, 0x62, 0x6c, 0x6f, 0x63, 0x6b, 0x73, 0x18, 0x09,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x06, 0x62, 0x6c, 0x6f, 0x63, 0x6b, 0x73, 0x22, 0xa2, 0x01, 0x0a,
	0x0d, 0x43, 0x68, 0x75, 0x6e, 0x6b, 0x4c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x1b,
	0x0a, 0x09, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x08, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x49, 0x64, 0x12, 0x18, 0x0a, 0x07, 0x61,
	0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x61, 0x64,
	0x64, 0x72, 0x65, 0x73, 0x73, 0x12, 0x18, 0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12,
	0x1d, 0x0a, 0x0a, 0x69, 0x73, 0x5f, 0x70, 0x72, 0x69, 0x6d, 0x61, 0x72, 0x79, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x08, 0x52, 0x09, 0x69, 0x73, 0x50, 0x72, 0x69, 0x6d, 0x61, 0x72, 0x79, 0x12, 0x21,
	0x0a, 0x0c, 0x6c, 0x65, 0x61, 0x73, 0x65, 0x5f, 0x65, 0x78, 0x70, 0x69, 0x72, 0x79, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x6c, 0x65, 0x61, 0x73, 0x65, 0x45, 0x78, 0x70, 0x69, 0x72,
	0x79, 0x42, 0x2e, 0x5a, 0x2c, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x75, 0x74, 0x74, 0x61, 0x6d, 0x2d, 0x6c, 0x69, 0x2f, 0x64, 0x66, 0x73, 0x2f, 0x61, 0x70, 0x69,
	0x2f, 0x67, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x64, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f,
	0x6e, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
})

var (
	file_api_proto_common_proto_rawDescOnce sync.Once
	file_api_proto_common_proto_rawDescData []byte
)

func file_api_proto_common_proto_rawDescGZIP() []byte {
	file_api_proto_common_proto_rawDescOnce.Do(func() {
		file_api_proto_common_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_api_proto_common_proto_rawDesc), len(file_api_proto_common_proto_rawDesc)))
	})
	return file_api_proto_common_proto_rawDescData
}

var file_api_proto_common_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_api_proto_common_proto_goTypes = []any{
	(*FileAttributes)(nil), // 0: dfs.FileAttributes
	(*ChunkLocation)(nil),  // 1: dfs.ChunkLocation
}
var file_api_proto_common_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_api_proto_common_proto_init() }
func file_api_proto_common_proto_init() {
	if File_api_proto_common_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_api_proto_common_proto_rawDesc), len(file_api_proto_common_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_api_proto_common_proto_goTypes,
		DependencyIndexes: file_api_proto_common_proto_depIdxs,
		MessageInfos:      file_api_proto_common_proto_msgTypes,
	}.Build()
	File_api_proto_common_proto = out.File
	file_api_proto_common_proto_goTypes = nil
	file_api_proto_common_proto_depIdxs = nil
}
