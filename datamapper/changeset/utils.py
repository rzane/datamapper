def dict_merge(dct: dict, merge_dct: dict) -> dict:
    dct_ = dict(dct)
    for k, v in merge_dct.items():
        dct_[k] = merge_dct[k]
    return dct_
