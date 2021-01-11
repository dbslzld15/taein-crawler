class TaeinStoreError(Exception):
    pass


class TaeinStoreS3NotFound(TaeinStoreError):
    pass


class TaeinStoreCrawlerLogNotFound(TaeinStoreError):
    pass


class TaeinStoreRegionNotFound(TaeinStoreError):
    pass
