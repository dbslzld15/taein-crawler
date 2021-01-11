class TaeinClientError(Exception):
    pass


class TaeinClientResponseError(TaeinClientError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(status_code, message)


class TaeinClientParseError(TaeinClientError):
    pass
