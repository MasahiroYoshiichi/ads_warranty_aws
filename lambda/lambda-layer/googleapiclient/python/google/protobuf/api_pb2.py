# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: google/protobuf/api.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import source_context_pb2 as google_dot_protobuf_dot_source__context__pb2
from google.protobuf import type_pb2 as google_dot_protobuf_dot_type__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x19google/protobuf/api.proto\x12\x0fgoogle.protobuf\x1a$google/protobuf/source_context.proto\x1a\x1agoogle/protobuf/type.proto\"\xc1\x02\n\x03\x41pi\x12\x12\n\x04name\x18\x01 \x01(\tR\x04name\x12\x31\n\x07methods\x18\x02 \x03(\x0b\x32\x17.google.protobuf.MethodR\x07methods\x12\x31\n\x07options\x18\x03 \x03(\x0b\x32\x17.google.protobuf.OptionR\x07options\x12\x18\n\x07version\x18\x04 \x01(\tR\x07version\x12\x45\n\x0esource_context\x18\x05 \x01(\x0b\x32\x1e.google.protobuf.SourceContextR\rsourceContext\x12.\n\x06mixins\x18\x06 \x03(\x0b\x32\x16.google.protobuf.MixinR\x06mixins\x12/\n\x06syntax\x18\x07 \x01(\x0e\x32\x17.google.protobuf.SyntaxR\x06syntax\"\xb2\x02\n\x06Method\x12\x12\n\x04name\x18\x01 \x01(\tR\x04name\x12(\n\x10request_type_url\x18\x02 \x01(\tR\x0erequestTypeUrl\x12+\n\x11request_streaming\x18\x03 \x01(\x08R\x10requestStreaming\x12*\n\x11response_type_url\x18\x04 \x01(\tR\x0fresponseTypeUrl\x12-\n\x12response_streaming\x18\x05 \x01(\x08R\x11responseStreaming\x12\x31\n\x07options\x18\x06 \x03(\x0b\x32\x17.google.protobuf.OptionR\x07options\x12/\n\x06syntax\x18\x07 \x01(\x0e\x32\x17.google.protobuf.SyntaxR\x06syntax\"/\n\x05Mixin\x12\x12\n\x04name\x18\x01 \x01(\tR\x04name\x12\x12\n\x04root\x18\x02 \x01(\tR\x04rootBv\n\x13\x63om.google.protobufB\x08\x41piProtoP\x01Z,google.golang.org/protobuf/types/known/apipb\xa2\x02\x03GPB\xaa\x02\x1eGoogle.Protobuf.WellKnownTypesb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'google.protobuf.api_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  _globals['DESCRIPTOR']._options = None
  _globals['DESCRIPTOR']._serialized_options = b'\n\023com.google.protobufB\010ApiProtoP\001Z,google.golang.org/protobuf/types/known/apipb\242\002\003GPB\252\002\036Google.Protobuf.WellKnownTypes'
  _globals['_API']._serialized_start=113
  _globals['_API']._serialized_end=434
  _globals['_METHOD']._serialized_start=437
  _globals['_METHOD']._serialized_end=743
  _globals['_MIXIN']._serialized_start=745
  _globals['_MIXIN']._serialized_end=792
# @@protoc_insertion_point(module_scope)
