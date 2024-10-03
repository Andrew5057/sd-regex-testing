import polars as pl

FIELDS = ('title', 'username', 'body', 'is_tp')

def read_json(source):
    posts = pl.read_json(source)
    # Disputed and unreviewed should be excluded
    posts = posts.filter(pl.col('is_tp').xor('is_fp'))
    return posts.select(FIELDS)

class SmokeDetectorRegexTestingFrame:
    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df
    
    def test_title(self, regex):
        regex = "(?i)" + regex
        return self._df.with_columns(
            matched=pl.col('title').str.contains(regex)
        )
        return posts
    
    def test_username(self, regex):
        regex = "(?i)" + regex
        return self._df.with_columns(
            matched=pl.col('username').str.contains(regex)
        )
    
    def test_keyword(self, regex):
        regex = r"(?i)(?:^|\b|(?w:\b))" + regex + r"(?:\b|(?w:\b)|$)"
        return self._df.with_columns(
            matched=pl.any_horizontal(
                pl.col(field).str.contains(regex) for field in FIELDS
            )
        )

    def test_website(self, regex):
        regex = r"(?i)" + regex
        return self._df.with_columns(
            matched=pl.any_horizontal(
                pl.col(field).str.contains(regex) for field in FIELDS
            )
        )
    
    @property
    def tp(self):
        return self._df.filter(pl.col('is_tp') & pl.col('matched'))
    
    @property
    def fp(self):
        return self._df.filter(~pl.col('is_tp') & pl.col('matched'))
    
    @property
    def tn(self):
        return self._df.filter(~pl.col('is_tp') & ~pl.col('matched'))
    
    @property
    def fn(self):
        return self._df.filter(pl.col('is_tp') & ~pl.col('matched'))

pl.api.register_dataframe_namespace("sdrt")(SmokeDetectorRegexTestingFrame)
pl.api.register_lazyframe_namespace("sdrt")(SmokeDetectorRegexTestingFrame)
