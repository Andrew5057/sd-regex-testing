"""An interactive CLI tool that wraps the sd-regex-testing library.
"""

import argparse
import polars as pl
import sd_regex_testing as sdrt

def test_from_cli():
    """Launch an interactive regex testing session.
    """
    parser = argparse.ArgumentParser(prog='sdrt', description="Interactively "
                                     + "test regexes against metasmoke data")
    parser.add_argument('file', type=str, help="the path to a metasmoke JSON file")
    args = parser.parse_args()
    
    session = InteractiveSession(args.file)
    
    session.start()

class InteractiveSession:
    def __init__(self, file: str):
        self.file = file
        self.posts = sdrt.read_json(file)
        self.last_regex = ""
        self.last_test_type = "placeholder"
    
    def start(self):
        print(f"""
Started an interactive SDRT session with {self.file} ({len(self.posts)} posts)
Commands:
- "test [title|username|keyword|website] [regex]" to store a regex test
- "[tp|fp|tn|fn]" to get the number of each type of result
- "summarize" to see a quick summary of the result
- "exit" or CTRL+C to exit""")
        try:
            while True:
                self._parse_command(input(">>> "))
        except KeyboardInterrupt:
            return
    
    def _parse_command(self, command: str):
        """Convert a command from an interactive session into its corresponding
        DataFrame output. Accepted inputs are:
        - "tp", "fp", "tn", "fn" (prints the filtered posts)
        - "test [title|username|keyword|website] [regex]" (stores the regex
        test in the posts field)
        
        :param command: The inputted line of text
        :type command: str
        """
        command_raw = command.split(maxsplit=2)
        match command_raw[0]:
            case "tp":
                print(len(self.posts.sdrt.tp))
                return
            case "fp":
                print(len(self.posts.sdrt.fp))
                return
            case "tn":
                print(len(self.posts.sdrt.tn))
                return
            case "fn":
                print(len(self.posts.sdrt.fn))
                return
            case "summarize":
                print(f"Regex '{self.last_regex}' as a "
                      + f"{self.last_test_type} yielded "
                      + f"{len(self.posts.sdrt.tp)} TP, "
                      + f"{len(self.posts.sdrt.fp)} FP, "
                      + f"{len(self.posts.sdrt.tn)} TN, "
                      + f"{len(self.posts.sdrt.fn)} FN")
            case "test":
                if len(command_raw) < 3:
                    return
                match command_raw[1]:
                    case "title":
                        self.posts = self.posts.sdrt.test_title(command_raw[2])
                    case "username":
                        self.posts = self.posts.sdrt.test_username(command_raw[2])
                    case "keyword":
                        self.posts = self.posts.sdrt.test_keyword(command_raw[2])
                    case "website":
                        self.posts = self.posts.sdrt.test_keyword(command_raw[2])
                    case _:
                        return
                self.last_regex = command_raw[2]
                self.last_test_type = command_raw[1]
                print(f"Tested '{self.last_regex}' as a {self.last_test_type}")
            case "exit":
                raise KeyboardInterrupt() # Gets caught by exception handling
