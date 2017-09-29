# Diesel-oci

A backend implementation for [diesel](https://github.com/diesel-rs/diesel) for oracle sql database. **This crate is currently functional and only a prove of concept**.

## Status:

- [x] Builds with diesel 0.16.0
- [x] Able to execute simplest queries (`SELECT * from table;`)

## Not working/TODO:

- [ ] Check `FromSql`/`ToSql`
- [ ] Fix generated sql
- [ ] Port/use diesels test suite
- [ ] Make everything work
- [ ] Publish to crates.io

## Code of conduct

Anyone who interacts with Diesel in any space, including but not limited to
this GitHub repository, must follow our [code of conduct](https://github.com/diesel-rs/diesel/blob/master/code_of_conduct.md).

## License

Licensed under either of these:

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   http://opensource.org/licenses/MIT)

### Contributing

Unless you explicitly state otherwise, any contribution you intentionally submit
for inclusion in the work, as defined in the Apache-2.0 license, shall be
dual-licensed as above, without any additional terms or conditions.
