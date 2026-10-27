import re
from html import unescape
from html.parser import HTMLParser


class _PlainTextParser(HTMLParser):
    _BLOCK_TAGS = {
        "address", "article", "aside", "blockquote", "br", "dd", "div", "dl",
        "dt", "figcaption", "figure", "footer", "h1", "h2", "h3", "h4", "h5",
        "h6", "header", "hr", "li", "main", "nav", "ol", "p", "pre", "section",
        "table", "td", "th", "tr", "ul",
    }

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.hidden_tags = []

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style"}:
            self.hidden_tags.append(tag)
        elif not self.hidden_tags and tag in self._BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if self.hidden_tags:
            if tag == self.hidden_tags[-1]:
                self.hidden_tags.pop()
        elif tag in self._BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.hidden_tags:
            self.parts.append(data)


def strip_html_text(value):
    if not isinstance(value, str) or ("<" not in value and "&" not in value):
        return value

    # Decode escaped markup before parsing it, including double-escaped exports.
    text = value
    for _ in range(3):
        decoded = unescape(text)
        if decoded == text:
            break
        text = decoded

    parser = _PlainTextParser()
    parser.feed(text)
    parser.close()
    text = "".join(parser.parts).replace("\xa0", " ")
    lines = (re.sub(r"[ \t\f\v]+", " ", line).strip() for line in text.splitlines())
    return "\n".join(line for line in lines if line)
