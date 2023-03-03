import copy


class SaveDocument:
    def __init__(self):
        self.image_nodes = {}
        self.text_nodes = {}
        self.url = {}
        self.lang = ""
        self.alt_detected = False
        self.prev_depth = None
        self.current_path_to_root = []

        # Def idx
        self.cur_txt_idx = "#000000"
        self.cur_img_idx = "#000000"
        self.node_idx = '0'

    def add_url(self, url):
        self.url[url] = 1

    def check_url(self, url):
        return True if url in self.url.keys() else False

    def is_valid(self):
        if not self.image_nodes:
            return False
        elif self.image_nodes and not self.alt_detected and not self.text_nodes:
            return False
        else:
            return True

    def increment_idx(self, text_node=False):
        if text_node:
            self.cur_txt_idx = "#" + str(int(self.cur_txt_idx[1:]) + 1).zfill(6)
        else:
            self.cur_img_idx = "#" + str(int(self.cur_img_idx[1:]) + 1).zfill(6)

    def increment_node_idx(self):
        self.node_idx = str(int(self.node_idx) + 1)

    def update_path_to_root(self, depth):
        self.current_path_to_root = self.current_path_to_root[:depth]


def build_graph(document: SaveDocument):
    img_copy = copy.deepcopy(document.image_nodes)
    txt_copy = copy.deepcopy(document.text_nodes)
    document.image_nodes = []
    for im_idx, im_node in img_copy.items():
        meta_text = []
        for txt_idx, text_node in txt_copy.items():
            nca = nearest_common_ancestor(im_node["path_to_root"], text_node["path_to_root"])
            meta_text.append({"text_idx": txt_idx,
                              "nearest_common_ancestor": nca,
                              "shortest_path": im_node["depth"] - (len(im_node["path_to_root"]) - nca) +
                                               text_node["depth"] - (len(text_node["path_to_root"]) - nca),
                              "is_parent": is_parent(im_node["path_to_root"], text_node["text_tree_id"]),
                              "relative_depth": im_node["depth"] - text_node["depth"]})
                
        im_node.pop("path_to_root")
        im_node["img_idx"] = im_idx
        im_node["meta_text"] = meta_text
        document.image_nodes.append(im_node)

        document.text_nodes = [{"text_idx": k, **{_k: _v for _k, _v in v.items() if _k != "path_to_root"}}
                               for k, v in txt_copy.items()]
    return document


def nearest_common_ancestor(path_root_img, path_root_txt):
    min_length = min(len(path_root_img), len(path_root_txt))
    for idx in range(min_length):
        if path_root_img[idx] != path_root_txt[idx]:
            return len(path_root_img) - (idx - 1)
    return 1


def is_parent(path_root_img, txt_node_idx):
    key_path = {k: 1 for k in path_root_img}
    return 1 if txt_node_idx in key_path.keys() else 0
