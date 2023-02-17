import sys
sys.setrecursionlimit(40000)
from tqdm import tqdm
from fastwarc.warc import ArchiveIterator, is_http
from fastwarc.stream_io import *
from bs4 import BeautifulSoup

file = "CC-MAIN-20220924151538-20220924181538-00000.warc.gz"
stream = GZipStream(open(file, 'rb'))


out = {}
for record in tqdm(ArchiveIterator(stream, func_filter=is_http)):
    if record.content_length > 1000:  # Bytes limit
        body = record.reader.read()
        soup = BeautifulSoup(body)
        imgs = []
        for img_sample in soup.find_all('img'):
            any_txt = False
            if "src" in img_sample.attrs.keys() and img_sample.attrs["src"].startswith("http"):

                sample_out = {"url": img_sample.attrs["src"]}

                # Alt-text
                if "alt" in img_sample.attrs.keys() and img_sample.attrs["alt"]:
                    any_txt = True
                    sample_out["alt"] = img_sample.attrs["alt"]

                # Paragraphs
                p_parents = img_sample.find_parents("p")
                p_parents = [parent.text for parent in p_parents if parent.text != "\n" and parent.text.strip() != ""]
                if p_parents:
                    any_txt = True
                    sample_out["p"] = p_parents

                # Headers
                parents_name = []
                for tree in img_sample.find_parents():
                    parents_name.append(tree.name)
                for head_tag in [f"h{str(i)}" for i in range(1, 7)]:
                    if head_tag in parents_name:
                        txt = img_sample.find_parent(head_tag).text
                        txt = txt.strip("\n").strip()
                        if txt:
                            any_txt = True
                            sample_out[head_tag] = txt

                # Save sample
                if any_txt:
                    imgs.append(sample_out)

        title = soup.title.text if soup.title is not None else ""
        if imgs:
            out[record.headers['WARC-Record-ID']] = {"title": "", "images": imgs}
