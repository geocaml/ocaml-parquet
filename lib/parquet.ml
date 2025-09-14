module Types = Parquet_types
module File = struct
  type ro = file_offset:Optint.Int63.t -> Cstruct.t list -> unit
  (** Read-only access to a file that supports reading at a particular offset.
      The read should be exact and raise [End_of_file] should that be the case.
  *)

  type with_file = string -> ro * int
  (** [with_file path] should return an {! ro} alongside the size of the file in
      bytes. *)
end

class transport buffer =
  object
    val mutable opened = true
    val mutable position = 0
    inherit Thrift.Transport.t
    method isOpen = opened
    method opn = ()
    method close = opened <- false

    method read buf off len =
      let module T = Thrift.Transport in
      if opened then
        try
          Cstruct.blit_to_bytes buffer position buf off len;
          position <- position + len;
          len
        with _ ->
          raise
            (T.E
               ( T.UNKNOWN,
                 "TChannelTransport: Could not read " ^ string_of_int len ))
      else raise (T.E (T.NOT_OPEN, "TChannelTransport: Channel was closed"))

    method write _buf _off _len = assert false
    method write_string _buf _off _len = assert false
    method flush = assert false
  end

let parse_metadata with_file file =
  let header = Cstruct.create 4 in
  with_file file @@ fun (ro, size) ->
  ro ~file_offset:Optint.Int63.zero [ header ];
  assert (String.equal "PAR1" (Cstruct.to_string header));
  let length_buf = Cstruct.create 4 in
  ro ~file_offset:Optint.Int63.(of_int (size - 8)) [ length_buf ];
  let length = Cstruct.LE.get_uint32 length_buf 0 |> Int32.to_int in
  let metadata_offset = size - 8 - length in
  let metadata_buf = Cstruct.create length in
  ro ~file_offset:(Optint.Int63.of_int metadata_offset) [ metadata_buf ];
  let compact = new TCompactProtocol.t (new transport metadata_buf) in
  Parquet_types.read_fileMetaData compact

let get_column_type col wf f =
  let metadata : Parquet_types.fileMetaData = parse_metadata wf f in
  let row_groups = metadata#grab_row_groups in
  List.find_map (fun (row : Parquet_types.rowGroup) ->
    let columns : Parquet_types.columnChunk list = row#grab_columns in
      List.find_map (fun (column : Parquet_types.columnChunk) ->
        let meta = column#grab_meta_data in
        if meta#grab_path_in_schema = col then Some meta#grab_type else None)
        columns) row_groups 
