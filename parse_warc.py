import sys
import os
import time
import pdb
import argparse
sys.setrecursionlimit(40000)
from tqdm import tqdm
from fastwarc.warc import ArchiveIterator, is_http
from fastwarc.stream_io import *
from resiliparse.parse.html import HTMLTree, traverse_dom, DOMNode, DOMContext
import multiprocessing as mp
from multiprocessing import Pool, cpu_count
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc
from data_structure import SaveDocument, build_graph
import fasttext
import copy


dir_model = os.path.join(os.environ.get("STORE"), "fastText")
model = fasttext.load_model(os.path.join(dir_model, "lid.176.bin"))


file = "CC-MAIN-20220924151538-20220924181538-00000.warc.gz"
path = os.path.join(os.environ.get("STORE"), "common-crawl-dumps", file)
stream = GZipStream(open(path, 'rb'))


def save_node(node_ctx: DOMContext, Document: SaveDocument):
    node = node_ctx.node
    depth = node_ctx.depth

    if node.attrs is not None:
        itemprop = node["itemprop"] if "itemprop" in node.attrs else ""
        itemtype = node["itemtype"] if "itemtype" in node.attrs else ""

    if Document.prev_depth is None:
        Document.prev_depth = depth
    if depth < len(Document.current_path_to_root):
        Document.update_path_to_root(depth)

    if node.tag == "img":
        if "src" in node.attrs and node["src"].startswith("http") and node["src"].endswith(
                ("jpg", "jpeg", "png")):

            sample = {"url": node["src"], "depth": depth, "alt": "", "itemprop": itemprop, "itemtype": itemtype,
                      "path_to_root": Document.current_path_to_root}

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
                Document.increment_idx(img_node=True)
    elif node.tag == "video":
        if "src" in node.attrs and node["src"].startswith("http"):

            sample = {"url": node["src"], "depth": depth, "itemprop": itemprop, "itemtype": itemtype,
                      "path_to_root": Document.current_path_to_root}
            if not Document.check_url(node["src"]):
                Document.add_url(node["src"])

                # Add video node to document
                vid_idx = Document.cur_vid_idx
                Document.video_nodes[vid_idx] = sample
                Document.increment_idx()
                Document.has_video = True

    elif node.tag == "iframe":
        if "src" in node.attrs and ("youtube" in node["src"] or "dailymotion" in node["src"]):

            sample = {"url": node["src"], "depth": depth, "itemprop": itemprop, "itemtype": itemtype,
                      "path_to_root": Document.current_path_to_root}

            if not Document.check_url(node["src"]):
                Document.add_url(node["src"])

                # Add video node to document
                vid_idx = Document.cur_vid_idx
                Document.video_nodes[vid_idx] = sample
                Document.increment_idx()
                Document.has_video = True
    else:
        if node.tag == "p" or (node.tag.startswith("h") and len(node.tag) == 2):
            if node.text:
                text = node.text.replace("\t", "").replace("\n", "").strip()
                if text:
                    if not Document.check_text(text):

                        sample = {"tag": node.tag, "depth": depth, "text": text, "text_tree_id": Document.node_idx,
                                  "itemprop": itemprop, "itemtype": itemtype, "path_to_root": copy.deepcopy(Document.current_path_to_root)}

                        lang_pred = ""
                        labels, scores = model.predict(text, 3)
                        for lab, s in zip(labels, scores):
                            if lang_pred:
                                lang_pred += "||"
                            lang_pred += lab + "||" + str(round(s, 4))[:5]

                        sample["lang_pred"] = lang_pred
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
            return {"warc_id": record.headers['WARC-Record-ID'],
                    "title": title,
                    "lang_id": Document.lang,
                    "has_video": Document.has_video,
                    "meta_image": Document.image_nodes,
                    "meta_video": Document.video_nodes,
                    "text": Document.text_nodes}
        else:
            return None
    else:
        return None


def main(params):
    warc_iter = ArchiveIterator(stream, func_filter=is_http)
    if not params.disable_multiprocessing:
        pool = Pool(params.num_proc)  # cpu_count() // 2
        iterator = pool.imap_unordered(process_html, warc_iter)
        iterator = tqdm(iterator, desc='Processing HTML')
        out = [sample for sample in iterator if sample is not None]
    else:
        out = []
        for record in tqdm(warc_iter, desc='Processing HTML'):
            sample = process_html(record)
            if sample is not None:
                out.append(sample)
    return out


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--disable_multiprocessing", action="store_true",
                        help="Disable multiprocessing")
    parser.add_argument("--debug", action="store_true",
                        help="debugging")
    parser.add_argument("--num_proc", type=int)
    params = parser.parse_args()

    if not params.debug:
        pdb.set_trace = lambda: None

    out = main(params)

    schema = pa.schema([
        ("warc_id", pa.string()),
        ("title", pa.string()),
        ("lang_id", pa.string()),
        ("has_video", pa.bool_()),
        ("meta_image", pa.list_(
            pa.struct([
                pa.field("url", pa.string()),
                pa.field("depth", pa.int32()),
                pa.field("alt", pa.string()),
                pa.field("itemprop", pa.string()),
                pa.field("itemtype", pa.string()),
                pa.field("img_idx", pa.string()),
                pa.field("meta_text", pa.list_(
                    pa.struct([
                        pa.field("text_idx", pa.string()),
                        pa.field("nearest_common_ancestor", pa.int32()),
                        pa.field("shortest_path", pa.int32()),
                        pa.field("is_parent", pa.int8()),
                        pa.field("relative_depth", pa.int32())
                    ])
                ))
            ])
        )),
        ("meta_video", pa.list_(
            pa.struct([
                pa.field("url", pa.string()),
                pa.field("depth", pa.int32()),
                pa.field("itemprop", pa.string()),
                pa.field("itemtype", pa.string()),
                pa.field("vid_idx", pa.string()),
                pa.field("meta_text", pa.list_(
                    pa.struct([
                        pa.field("text_idx", pa.string()),
                        pa.field("nearest_common_ancestor", pa.int32()),
                        pa.field("shortest_path", pa.int32()),
                        pa.field("is_parent", pa.int8()),
                        pa.field("relative_depth", pa.int32())
                    ])
                ))
            ])
        )),
        ("text", pa.list_(
            pa.struct([
                pa.field("text_idx", pa.string()),
                pa.field("tag", pa.string()),
                pa.field("depth", pa.int32()),
                pa.field("itemprop", pa.string()),
                pa.field("itemtype", pa.string()),
                pa.field("text", pa.string()),
                pa.field("text_tree_id", pa.string()),
                pa.field("lang_pred", pa.string())
            ])
        ))
    ])

    # Convert the nested dictionary to a PyArrow Table
    table = pa.Table.from_pylist(out, schema=schema)
    pdb.set_trace()
    # table.take([idx_row])
    # table.select(["meta_video"])
    # mask = list(table.select(["has_video"]).to_pydict().values())[0]
    # vid_table = table.filter(mask)
    # vid_table.select(["meta_video"]).take([50]).to_pydict()
    # vid_table.select(["text"]).take([50]).to_pydict()
