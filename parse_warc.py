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
from data_structure import SaveDocument, build_graph


file = "CC-MAIN-20220924151538-20220924181538-00000.warc.gz"
path = os.path.join(os.environ.get("STORE"), "common-crawl-dumps", file)
stream = GZipStream(open(path, 'rb'))


def save_node(node_ctx: DOMContext, Document: SaveDocument):
    node = node_ctx.node
    depth = node_ctx.depth
    if Document.prev_depth is None:
        Document.prev_depth = depth
    if depth < len(Document.current_path_to_root):
        Document.update_path_to_root(depth)

    if node.tag == "img":
        if "src" in node.attrs and node["src"].startswith("http") and node["src"].endswith(
                ("jpg", "jpeg", "png")):
            sample = {"url": node["src"], "depth": depth, "alt": "", "path_to_root": Document.current_path_to_root}
            if not Document.check_url(node["src"]):
                Document.add_url(node["src"])
                if "alt" in node.attrs:
                    _alt = node["alt"].strip().strip('\n')
                    if _alt:
                        Document.alt_detected = True
                        sample["alt"] = _alt

                # Add image node to document
                img_idx = Document.cur_img_idx
                Document.image_nodes[img_idx] = sample
                Document.increment_idx()
    else:
        if node.tag == "p" or (node.tag.startswith("h") and len(node.tag) == 2):
            if node.text:
                text = node.text.strip().strip("\n")
                text = text.replace("\t", "").replace("\n", "").strip()
                if text:
                    sample = {"tag": node.tag, "depth": depth, "text": text, "text_tree_id": Document.node_idx,
                              "path_to_root": Document.current_path_to_root}

                    # Add text node to document
                    txt_idx = Document.cur_txt_idx
                    Document.text_nodes[txt_idx] = sample
                    Document.increment_idx(text_node=True)

    Document.current_path_to_root.append(Document.node_idx)
    Document.increment_node_idx()


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
            Document = build_graph(Document)
            return {record.headers['WARC-Record-ID']: {"title": title, "document": Document}}
        else:
            return None
    else:
        return None


def main(params):
    warc_iter = ArchiveIterator(stream, func_filter=is_http)
    if not params.disable_multiprocessing:
        pool = Pool(cpu_count())
        iterator = pool.imap_unordered(process_html, warc_iter)
        iterator = tqdm(iterator, desc='Processing HTML')
        out = {k: v for sample in iterator if sample is not None for k, v in sample.items()}
    else:
        out = {}
        for record in tqdm(warc_iter, desc='Processing HTML'):
            sample = process_html(record)
            if sample is not None:
                out.update(sample)
    return out


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--disable_multiprocessing", action="store_true",
                        help="Disable multiprocessing")
    parser.add_argument("--debug", action="store_true",
                        help="debugging")
    params = parser.parse_args()

    if not params.debug:
        pdb.set_trace = lambda: None

    out = main(params)
