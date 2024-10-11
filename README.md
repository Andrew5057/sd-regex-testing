# SmokeDetector Regex Testing

---

SmokeDetector Regex Testing (SDRT) provides functionality for testing regexes
against [metasmoke](https://m.erwaysoftware.com) data and analyzing their results.

It makes heavy use of the [Polars](https://pola.rs) library, which it uses to
store post data, test regexes, filter results, and more.


## Usage

It's recommended to use the `sdrt` alias when importing `sd_regex_testing`:

```python
import sd_regex_testing as sdrt
```

Any processing first requires a call to `sdrt.read_json`. This function accepts
the path to a metasmoke JSON file and returns a Polars DataFrame.

```python
data = sdrt.read_json("path/to/file")
```

From there, the DataFrame can be tested against a regex. The `sdrt` polars
namespace includes several testing methods:

```python
title = data.sdrt.test_title("test")
username = data.sdrt.test_username("test")
keyword = data.sdrt.test_keyword("test")
website = data.sdrt.test_website("test")
```

Each of these methods also takes a `case_sensitive` optional parameter, which
defaults to `False`.

```python
case_sensitive = data.sdrt.test_keyword("test", case_sensitive=True)
```

The results of a given test can be filtered using the `tp`, `fp`, `tn`, and
`fn` properties, which reflect the effectiveness of the just-tested regex.

```python
tps = keyword.sdrt.tp
fps = keyword.sdrt.fp
tns = keyword.sdrt.tn
fns = keyword.sdrt.fn
```
