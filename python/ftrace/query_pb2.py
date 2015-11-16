# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: query.proto

from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import service as _service
from google.protobuf import service_reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)




DESCRIPTOR = _descriptor.FileDescriptor(
  name='query.proto',
  package='mdt.SearchEngine',
  serialized_pb='\n\x0bquery.proto\x12\x10mdt.SearchEngine\"l\n\x11RpcIndexCondition\x12\x18\n\x10index_table_name\x18\x01 \x01(\t\x12,\n\x03\x63mp\x18\x02 \x01(\x0e\x32\x1f.mdt.SearchEngine.RpcComparator\x12\x0f\n\x07\x63mp_key\x18\x03 \x01(\t\"\xc3\x01\n\x10RpcSearchRequest\x12\x0f\n\x07\x64\x62_name\x18\x01 \x01(\t\x12\x12\n\ntable_name\x18\x02 \x01(\t\x12\x13\n\x0bprimary_key\x18\x03 \x01(\t\x12\x36\n\tcondition\x18\x04 \x03(\x0b\x32#.mdt.SearchEngine.RpcIndexCondition\x12\x17\n\x0fstart_timestamp\x18\x05 \x01(\x04\x12\x15\n\rend_timestamp\x18\x06 \x01(\x04\x12\r\n\x05limit\x18\x07 \x01(\x05\"9\n\x0fRpcResultStream\x12\x13\n\x0bprimary_key\x18\x01 \x01(\t\x12\x11\n\tdata_list\x18\x02 \x03(\t\"K\n\x11RpcSearchResponse\x12\x36\n\x0bresult_list\x18\x01 \x03(\x0b\x32!.mdt.SearchEngine.RpcResultStream\":\n\x13RpcOpenTableRequest\x12\x0f\n\x07\x64\x62_name\x18\x01 \x01(\t\x12\x12\n\ntable_name\x18\x02 \x01(\t\"K\n\x14RpcOpenTableResponse\x12\x33\n\x06status\x18\x01 \x01(\x0e\x32#.mdt.SearchEngine.RpcResponseStatus\")\n\x16RpcOpenDatabaseRequest\x12\x0f\n\x07\x64\x62_name\x18\x01 \x01(\t\"N\n\x17RpcOpenDatabaseResponse\x12\x33\n\x06status\x18\x01 \x01(\x0e\x32#.mdt.SearchEngine.RpcResponseStatus*G\n\x11RpcResponseStatus\x12\t\n\x05RpcOK\x10\x01\x12\x14\n\x10RpcTableNotFound\x10\x02\x12\x11\n\rRpcNotSupport\x10\x03*v\n\rRpcComparator\x12\x0e\n\nRpcEqualTo\x10\x00\x12\x11\n\rRpcNotEqualTo\x10\x01\x12\x0b\n\x07RpcLess\x10\x02\x12\x10\n\x0cRpcLessEqual\x10\x03\x12\x0e\n\nRpcGreater\x10\x04\x12\x13\n\x0fRpcGreaterEqual\x10\x05\x32\xa9\x02\n\x13SearchEngineService\x12Q\n\x06Search\x12\".mdt.SearchEngine.RpcSearchRequest\x1a#.mdt.SearchEngine.RpcSearchResponse\x12Z\n\tOpenTable\x12%.mdt.SearchEngine.RpcOpenTableRequest\x1a&.mdt.SearchEngine.RpcOpenTableResponse\x12\x63\n\x0cOpenDatabase\x12(.mdt.SearchEngine.RpcOpenDatabaseRequest\x1a).mdt.SearchEngine.RpcOpenDatabaseResponseB\x06\x80\x01\x01\x90\x01\x01')

_RPCRESPONSESTATUS = _descriptor.EnumDescriptor(
  name='RpcResponseStatus',
  full_name='mdt.SearchEngine.RpcResponseStatus',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='RpcOK', index=0, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='RpcTableNotFound', index=1, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='RpcNotSupport', index=2, number=3,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=737,
  serialized_end=808,
)

RpcResponseStatus = enum_type_wrapper.EnumTypeWrapper(_RPCRESPONSESTATUS)
_RPCCOMPARATOR = _descriptor.EnumDescriptor(
  name='RpcComparator',
  full_name='mdt.SearchEngine.RpcComparator',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='RpcEqualTo', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='RpcNotEqualTo', index=1, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='RpcLess', index=2, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='RpcLessEqual', index=3, number=3,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='RpcGreater', index=4, number=4,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='RpcGreaterEqual', index=5, number=5,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=810,
  serialized_end=928,
)

RpcComparator = enum_type_wrapper.EnumTypeWrapper(_RPCCOMPARATOR)
RpcOK = 1
RpcTableNotFound = 2
RpcNotSupport = 3
RpcEqualTo = 0
RpcNotEqualTo = 1
RpcLess = 2
RpcLessEqual = 3
RpcGreater = 4
RpcGreaterEqual = 5



_RPCINDEXCONDITION = _descriptor.Descriptor(
  name='RpcIndexCondition',
  full_name='mdt.SearchEngine.RpcIndexCondition',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='index_table_name', full_name='mdt.SearchEngine.RpcIndexCondition.index_table_name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='cmp', full_name='mdt.SearchEngine.RpcIndexCondition.cmp', index=1,
      number=2, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='cmp_key', full_name='mdt.SearchEngine.RpcIndexCondition.cmp_key', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=33,
  serialized_end=141,
)


_RPCSEARCHREQUEST = _descriptor.Descriptor(
  name='RpcSearchRequest',
  full_name='mdt.SearchEngine.RpcSearchRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='db_name', full_name='mdt.SearchEngine.RpcSearchRequest.db_name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='table_name', full_name='mdt.SearchEngine.RpcSearchRequest.table_name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='primary_key', full_name='mdt.SearchEngine.RpcSearchRequest.primary_key', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='condition', full_name='mdt.SearchEngine.RpcSearchRequest.condition', index=3,
      number=4, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='start_timestamp', full_name='mdt.SearchEngine.RpcSearchRequest.start_timestamp', index=4,
      number=5, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='end_timestamp', full_name='mdt.SearchEngine.RpcSearchRequest.end_timestamp', index=5,
      number=6, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='limit', full_name='mdt.SearchEngine.RpcSearchRequest.limit', index=6,
      number=7, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=144,
  serialized_end=339,
)


_RPCRESULTSTREAM = _descriptor.Descriptor(
  name='RpcResultStream',
  full_name='mdt.SearchEngine.RpcResultStream',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='primary_key', full_name='mdt.SearchEngine.RpcResultStream.primary_key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='data_list', full_name='mdt.SearchEngine.RpcResultStream.data_list', index=1,
      number=2, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=341,
  serialized_end=398,
)


_RPCSEARCHRESPONSE = _descriptor.Descriptor(
  name='RpcSearchResponse',
  full_name='mdt.SearchEngine.RpcSearchResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='result_list', full_name='mdt.SearchEngine.RpcSearchResponse.result_list', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=400,
  serialized_end=475,
)


_RPCOPENTABLEREQUEST = _descriptor.Descriptor(
  name='RpcOpenTableRequest',
  full_name='mdt.SearchEngine.RpcOpenTableRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='db_name', full_name='mdt.SearchEngine.RpcOpenTableRequest.db_name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='table_name', full_name='mdt.SearchEngine.RpcOpenTableRequest.table_name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=477,
  serialized_end=535,
)


_RPCOPENTABLERESPONSE = _descriptor.Descriptor(
  name='RpcOpenTableResponse',
  full_name='mdt.SearchEngine.RpcOpenTableResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='status', full_name='mdt.SearchEngine.RpcOpenTableResponse.status', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=1,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=537,
  serialized_end=612,
)


_RPCOPENDATABASEREQUEST = _descriptor.Descriptor(
  name='RpcOpenDatabaseRequest',
  full_name='mdt.SearchEngine.RpcOpenDatabaseRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='db_name', full_name='mdt.SearchEngine.RpcOpenDatabaseRequest.db_name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=614,
  serialized_end=655,
)


_RPCOPENDATABASERESPONSE = _descriptor.Descriptor(
  name='RpcOpenDatabaseResponse',
  full_name='mdt.SearchEngine.RpcOpenDatabaseResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='status', full_name='mdt.SearchEngine.RpcOpenDatabaseResponse.status', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=1,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=657,
  serialized_end=735,
)

_RPCINDEXCONDITION.fields_by_name['cmp'].enum_type = _RPCCOMPARATOR
_RPCSEARCHREQUEST.fields_by_name['condition'].message_type = _RPCINDEXCONDITION
_RPCSEARCHRESPONSE.fields_by_name['result_list'].message_type = _RPCRESULTSTREAM
_RPCOPENTABLERESPONSE.fields_by_name['status'].enum_type = _RPCRESPONSESTATUS
_RPCOPENDATABASERESPONSE.fields_by_name['status'].enum_type = _RPCRESPONSESTATUS
DESCRIPTOR.message_types_by_name['RpcIndexCondition'] = _RPCINDEXCONDITION
DESCRIPTOR.message_types_by_name['RpcSearchRequest'] = _RPCSEARCHREQUEST
DESCRIPTOR.message_types_by_name['RpcResultStream'] = _RPCRESULTSTREAM
DESCRIPTOR.message_types_by_name['RpcSearchResponse'] = _RPCSEARCHRESPONSE
DESCRIPTOR.message_types_by_name['RpcOpenTableRequest'] = _RPCOPENTABLEREQUEST
DESCRIPTOR.message_types_by_name['RpcOpenTableResponse'] = _RPCOPENTABLERESPONSE
DESCRIPTOR.message_types_by_name['RpcOpenDatabaseRequest'] = _RPCOPENDATABASEREQUEST
DESCRIPTOR.message_types_by_name['RpcOpenDatabaseResponse'] = _RPCOPENDATABASERESPONSE

class RpcIndexCondition(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _RPCINDEXCONDITION

  # @@protoc_insertion_point(class_scope:mdt.SearchEngine.RpcIndexCondition)

class RpcSearchRequest(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _RPCSEARCHREQUEST

  # @@protoc_insertion_point(class_scope:mdt.SearchEngine.RpcSearchRequest)

class RpcResultStream(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _RPCRESULTSTREAM

  # @@protoc_insertion_point(class_scope:mdt.SearchEngine.RpcResultStream)

class RpcSearchResponse(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _RPCSEARCHRESPONSE

  # @@protoc_insertion_point(class_scope:mdt.SearchEngine.RpcSearchResponse)

class RpcOpenTableRequest(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _RPCOPENTABLEREQUEST

  # @@protoc_insertion_point(class_scope:mdt.SearchEngine.RpcOpenTableRequest)

class RpcOpenTableResponse(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _RPCOPENTABLERESPONSE

  # @@protoc_insertion_point(class_scope:mdt.SearchEngine.RpcOpenTableResponse)

class RpcOpenDatabaseRequest(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _RPCOPENDATABASEREQUEST

  # @@protoc_insertion_point(class_scope:mdt.SearchEngine.RpcOpenDatabaseRequest)

class RpcOpenDatabaseResponse(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _RPCOPENDATABASERESPONSE

  # @@protoc_insertion_point(class_scope:mdt.SearchEngine.RpcOpenDatabaseResponse)


DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(descriptor_pb2.FileOptions(), '\200\001\001\220\001\001')

_SEARCHENGINESERVICE = _descriptor.ServiceDescriptor(
  name='SearchEngineService',
  full_name='mdt.SearchEngine.SearchEngineService',
  file=DESCRIPTOR,
  index=0,
  options=None,
  serialized_start=931,
  serialized_end=1228,
  methods=[
  _descriptor.MethodDescriptor(
    name='Search',
    full_name='mdt.SearchEngine.SearchEngineService.Search',
    index=0,
    containing_service=None,
    input_type=_RPCSEARCHREQUEST,
    output_type=_RPCSEARCHRESPONSE,
    options=None,
  ),
  _descriptor.MethodDescriptor(
    name='OpenTable',
    full_name='mdt.SearchEngine.SearchEngineService.OpenTable',
    index=1,
    containing_service=None,
    input_type=_RPCOPENTABLEREQUEST,
    output_type=_RPCOPENTABLERESPONSE,
    options=None,
  ),
  _descriptor.MethodDescriptor(
    name='OpenDatabase',
    full_name='mdt.SearchEngine.SearchEngineService.OpenDatabase',
    index=2,
    containing_service=None,
    input_type=_RPCOPENDATABASEREQUEST,
    output_type=_RPCOPENDATABASERESPONSE,
    options=None,
  ),
])

class SearchEngineService(_service.Service):
  __metaclass__ = service_reflection.GeneratedServiceType
  DESCRIPTOR = _SEARCHENGINESERVICE
class SearchEngineService_Stub(SearchEngineService):
  __metaclass__ = service_reflection.GeneratedServiceStubType
  DESCRIPTOR = _SEARCHENGINESERVICE

# @@protoc_insertion_point(module_scope)
