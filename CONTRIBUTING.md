## 🙌 Contributing

Contributions are what make the open-source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

### Ways to Contribute

- 🐛 **Report Bugs**: Open an issue with detailed reproduction steps
- 💡 **Suggest Features**: Share ideas for new functionality
- 📝 **Improve Documentation**: Help make docs clearer and more comprehensive
- 🔧 **Submit Code**: Fix bugs, implement features, or optimize performance
- 🧪 **Add Tests**: Improve test coverage and reliability
- 📊 **Performance Testing**: Benchmark and optimize the system

### Development Process

1. **Fork the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/MerkleKV.git
   cd MerkleKV
   ```

2. **Create a feature branch**
   ```bash
   git checkout -b feature/amazing-feature
   # or
   git checkout -b fix/bug-description
   ```

3. **Make your changes**
   ```bash
   # Follow Rust conventions
   cargo fmt
   cargo clippy
   cargo test
   ```

4. **Commit your changes**
   ```bash
   git commit -m "feat: add amazing feature"
   # Use conventional commit format:
   # feat: new feature
   # fix: bug fix
   # docs: documentation changes
   # test: add or update tests
   # refactor: code refactoring
   ```

5. **Push and create Pull Request**
   ```bash
   git push origin feature/amazing-feature
   ```

### Code Style & Guidelines

- Follow [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Use `cargo fmt` for formatting
- Use `cargo clippy` for linting
- Add tests for new functionality
- Update documentation for API changes
- Keep commits atomic and well-described

### Testing

```bash
# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test
cargo test test_merkle_tree

# Run integration tests
cargo test --test integration_tests
```

### Getting Help

- 💬 **Discussions**: Use GitHub Discussions for questions
- 🐛 **Issues**: Report bugs and feature requests
- 📧 **Email**: Contact maintainers for sensitive issues

For major changes, please open an issue first to discuss what you would like to change.
