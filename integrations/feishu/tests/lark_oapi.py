"""Minimal test double for importing the bridge without Feishu credentials."""

class _Namespace:
    def __getattr__(self, _name):
        return self

    def __call__(self, *args, **kwargs):
        return self


im = _Namespace()
cardkit = _Namespace()
contact = _Namespace()
ws = _Namespace()
Client = _Namespace()
EventDispatcherHandler = _Namespace()
LogLevel = _Namespace()
