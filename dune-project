(lang dune 3.19)

(name parquet)

(generate_opam_files true)

(source
 (github geocaml/ocaml-parquet))

(authors "Patrick Ferris <patrick@sirref.org>")

(maintainers "Patrick Ferris <patrick@sirref.org>")

(license ISC)

(package
 (name parquet)
 (synopsis "Parquet parsing")
 (description "A pure OCaml library for parsing parquet files")
 (depends
   ocaml
   optint
   thrift
   cstruct
   (alcotest :with-test)
 )
 (tags
  ("add topics" "to describe" your project)))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/reference/dune-project/index.html
