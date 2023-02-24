import sys
import os
import pdb
import argparse
sys.setrecursionlimit(40000)
from tqdm import tqdm
from fastwarc.warc import ArchiveIterator, is_http
from fastwarc.stream_io import *
from resiliparse.parse.html import HTMLTree, traverse_dom, DOMNode, DOMContext
from multiprocessing import Pool, cpu_count
import pyarrow.parquet as pq
import pyarrow as pa


file = "CC-MAIN-20220924151538-20220924181538-00000.warc.gz"
path = os.path.join(os.environ.get("STORE"), "common-crawl-dumps", file)
stream = GZipStream(open(path, 'rb'))


class SaveDocument:
    def __init__(self):
        self.document = []
        self.images = []
        self.url = {}
        self.lang = None
        self.alt_detected = False

    def add_url(self, url):
        self.url[url] = 1

    def check_url(self, url):
        return True if url in self.url.keys() else False

    def is_valid(self):
        if not self.images:
            return False
        elif self.images and not self.alt_detected and not self.document:
            return False
        else:
            return True


def save_node(node_ctx: DOMContext, Document: SaveDocument):
    node = node_ctx.node
    depth = node_ctx.depth
    pdb.set_trace()
    if node.tag == "img":
        if "src" in node.attrs and node["src"].startswith("http") and node["src"].endswith(
                ("jpg", "jpeg", "png")):
            sample = {"url": node["src"], "depth": depth}
            if not Document.check_url(node["src"]):
                Document.add_url(node["src"])
                if "alt" in node.attrs:
                    _alt = node["alt"].strip().strip('\n')
                    if _alt:
                        Document.alt_detected = True
                        sample["alt"] = _alt
                Document.images.append(sample)
    else:
        if node.tag == "p" or (node.tag.startswith("h") and len(node.tag) == 2):
            if node.text:
                text = node.text.strip().strip("\n")
                text = text.replace("\t", "").replace("\n", "").strip()
                if text:
                    Document.document.append({"tag": node.tag, "depth": depth, "text": text})


def process_html(record):
    if record.content_length > 1000:  # Bytes limit
        body = record.reader.read()
        tree = HTMLTree.parse_from_bytes(body)
        Document = SaveDocument()
        # Extract lang
        if "lang" in tree.document.get_elements_by_tag_name("html")[0].attrs:
            Document.lang = tree.document.get_elements_by_tag_name("html")[0]["lang"]
        traverse_dom(base_node=tree.document, start_callback=lambda node: save_node(node, Document), elements_only=False)
        title = tree.title.strip().strip("\n")
        if Document.is_valid():
            return {record.headers['WARC-Record-ID']: {"title": title, "document": Document}}
        else:
            return None
    else:
        return None


def main(params):
    pool = Pool(cpu_count())
    warc_iter = ArchiveIterator(stream, func_filter=is_http)
    if not params.disable_multiprocessing:
        iterator = pool.imap_unordered(process_html, warc_iter)
        iterator = tqdm(iterator, desc='Processing HTML')
        out = {k: v for sample in iterator if sample is not None for k, v in sample.items()}
    else:
        out = {}
        for record in tqdm(warc_iter, desc='Processing HTML'):
            sample = process_html(record)
            out.update(sample)
    return out


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--disable_multiprocessing", action="store_true",
                        help="Disable multiprocessing")
    parser.add_argument("--debug", action="store_true",
                        help="debugging")
    params = parser.parse_args()

    if params.debug:
        pdb.set_trace = lambda: None

    out = main(params)
