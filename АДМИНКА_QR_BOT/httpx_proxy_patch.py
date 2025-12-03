# utils/httpx_proxy_patch.py
import functools, httpx

def _patch(cls):
    orig_init = cls.__init__
    @functools.wraps(orig_init)
    def wrap(self, *args, **kw):
        if "proxy" in kw and "proxies" not in kw:
            kw["proxies"] = kw.pop("proxy")
        return orig_init(self, *args, **kw)
    cls.__init__ = wrap

for _c in (httpx.Client, httpx.AsyncClient):
    _patch(_c)