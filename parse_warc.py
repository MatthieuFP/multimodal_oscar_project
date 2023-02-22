import sys
sys.setrecursionlimit(40000)
from tqdm import tqdm
from fastwarc.warc import ArchiveIterator, is_http
from fastwarc.stream_io import *
from resiliparse.parse.html import HTMLTree, traverse_dom, DOMNode, DOMContext

file = "CC-MAIN-20220924151538-20220924181538-00000.warc.gz"
stream = GZipStream(open(file, 'rb'))


class SaveDocument:
    def __init__(self):
        self.document = []
        self.images = []
        self.url = {}
        self.lang = None
    def add_url(self, url):
        self.url[url] = 1
    def check_url(self, url):
        return True if url in self.url.keys() else False


def save_node(node_ctx: DOMContext):
    global img_tag, txt_tag
    node = node_ctx.node
    depth = node_ctx.depth
    if node.tag == "img":
        if "src" in node.attrs and node["src"].startswith("http") and node["src"].endswith(
                ("jpg", "jpeg", "png")):
            img_tag = True
            sample = {"url": node["src"], "depth": depth}
            if not Document.check_url(node["src"]):
                Document.add_url(node["src"])
                if "alt" in node.attrs:
                    _alt = node["alt"].strip().strip('\n')
                    if _alt:
                        txt_tag = True
                        sample["alt"] = _alt
                Document.images.append(sample)
    else:
        if node.tag == "p" or (node.tag.startswith("h") and len(node.tag) == 2):
            if node.text:
                text = node.text.strip().strip("\n")
                text = text.replace("\t", "").replace("\n", "").strip()
                if text:
                    txt_tag = True
                    Document.document.append({"tag": node.tag, "depth": depth, "text": text})


out = {}
for record in tqdm(ArchiveIterator(stream, func_filter=is_http)):
    img_tag, txt_tag = False, False
    if record.content_length > 1000:  # Bytes limit
        body = record.reader.read()
        tree = HTMLTree.parse_from_bytes(body)
        imgs = []
        Document = SaveDocument()
        # Extract lang
        if "lang" in tree.document.get_elements_by_tag_name("html")[0].attrs:
            Document.lang = tree.document.get_elements_by_tag_name("html")[0]["lang"]
        traverse_dom(base_node=tree.document, start_callback=save_node, elements_only=False)
        title = tree.title.strip().strip("\n")
        if img_tag and txt_tag:
            out[record.headers['WARC-Record-ID']] = {"title": title, "document": Document}
