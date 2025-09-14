let with_file f fn =
  let sz = (Unix.stat f).st_size in
  Parquet_unix.with_open_in f @@ fun ro -> fn (ro, sz)

let typ = Alcotest.of_pp (fun ppf v -> Fmt.int32 ppf (Parquet.Types.Type.to_i v))

let test_simple_parquet () =
  let data = "fixtures/belfast_bottle_banks.parquet" in
  let metadata =
    Parquet.parse_metadata with_file data
  in
  Alcotest.(check (option int64)) "same rows" (Some 45L) metadata#get_num_rows;
  let type' = Parquet.get_column_type [ "NAME" ] with_file data in
  Alcotest.(check (option typ)) "same type" (Some Parquet.Types.Type.BYTE_ARRAY) type' 

let () =
  Alcotest.run "parquet"
    [ ("parsing", [ Alcotest.test_case "simple" `Quick test_simple_parquet ]) ]
