
class MacroNotFoundOrInvalid(Exception):
    """
    Raised when fetching or processing a search macro definition from Splunk
    """

    def __init__(self, message: str = "Macro not found", macro="", returned_status=None):
        self.message = message
        self.macro = macro
        self.returned_status = returned_status

    def __str__(self):
        if self.macro:
            return f"{self.message} for {self.macro}"

        return str(self.message)



class SavedSearchNotFoundOrInvalid(Exception):
    """
    Raised when fetching or processing a saved search definition from Splunk
    """

    def __init__(self, message: str = "Saved search not found", search_name="", returned_status=None):
        self.message = message
        self.search_name = search_name
        self.returned_status = returned_status

    def __str__(self):
        if self.search_name:
            return f"{self.message} for {self.search_name}"
        return str(self.message)

