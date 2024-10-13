# SmokeDetector Regex Testing

---

SmokeDetector Regex Testing (SDRT) provides functionality for testing regexes
against [metasmoke](https://m.erwaysoftware.com) data and analyzing their results.

It makes heavy use of the [Polars](https://pola.rs) library, which it uses to
store post data, test regexes, filter results, and more.

## Installation

SDRT is available on PyPI under the name `sd-regex-testing`.

```bash
pip install sd-regex-testing
```

## Usage

### Python module

It's recommended to use the `sdrt` alias when importing `sd_regex_testing`:

```python
import sd_regex_testing as sdrt
```

Any processing requires an initial call to `sdrt.read_json`. This function
accepts the path to a metasmoke JSON file and returns a Polars DataFrame.

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
Each of these properties is a DataFrame containing only the target posts.

```python
tps = keyword.sdrt.tp
fps = keyword.sdrt.fp
tns = keyword.sdrt.tn
fns = keyword.sdrt.fn
```

### Commmand line tool

This package also creates an `sdrt` command line tool. It takes the path to
an MS JSON file as an argument:

```bash
sdrt path/to/file
```

This will open an interactive regex testing session. The `test` command will
test a given regex against the file and store the result.

```bash
>>> test (title|username|keyword|website) regex
```

The `tp`, `fp`, `tn`, and `fn` commands will report the number of posts
with the given result.

```bash
>>> tp|fp|tn|fn
```

The `summarize` command will pretty-print the counts for all four result
types, as well as reporting the last test.

```
>>> summarize
Regex [last regex] as a [last regex type] yielded [count] TP, [count] FP, [count] TN, and [count] FN.
```
