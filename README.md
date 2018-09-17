# Diesel-oci

**Work in progress. Not ready for general consumption.**.

A Oracle SQL database backend implementation for
[Diesel](https://github.com/diesel-rs/diesel).

## Status:

- [x] Builds with Diesel 1.3.2.
- [x] Support for DML statements (`SELECT`, `INSERT`, `UPDATE`, `DELETE`).
- [x] Support for Diesel `sql_types`: `Bool`, `SmallInt`,
      `Integer`, `Bigint`, `Float`, `Double`.
- [x] Limited Support for `Text`. Values up to `2 MB` are supported.
- [x] Limited Support for `Date`, `Time`, `Timestamp`.
      Currently no fractional second support.

## Not working/TODO:

- [ ] Support arbitrary sizes for `Binary`.
- [ ] Support `String` values > `2 MB`.
- [ ] Support fractional seconds for `Time` and `Timestamp`.
- [ ] Publish to crates.io.

## Code of conduct

Anyone who interacts with Diesel in any space, including but not limited to
this GitHub repository, must follow our [code of conduct](https://github.com/diesel-rs/diesel/blob/master/code_of_conduct.md).

## License

Licensed under either of these:

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
  http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  http://opensource.org/licenses/MIT)

### Contributing

Unless you explicitly state otherwise, any contribution you intentionally submit
for inclusion in the work, as defined in the Apache-2.0 license, shall be
dual-licensed as above, without any additional terms or conditions.
