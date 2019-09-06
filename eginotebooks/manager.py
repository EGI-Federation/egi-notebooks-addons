"""A contents manager that combine multiple content managers."""

# Copyright (c) IPython Development Team.
#           (c) EGI Foundation
# Distributed under the terms of the Modified BSD License.
import os.path

from traitlets.traitlets import List, Unicode, Dict
from traitlets import import_item

from notebook.services.contents.largefilemanager import LargeFileManager


def _split_path(path):
    """split a path return by the api
    return
        - the sentinel:
        - the rest of the path as a list.
        - the original path stripped of / for normalisation.
    """
    path = path.strip("/")
    list_path = path.split("/")
    sentinel = list_path.pop(0)
    return sentinel, list_path, path


# Base class will be responsible for handling those requests outside the
# "mixed_path", using LargeFileManager as it's the default in the current
# notebooks implementation
class MixedContentsManager(LargeFileManager):
    mixed_path = Unicode(
        "datahub", help="path were the mixed content managers will reside", config=True
    )
    filesystem_scheme = List(
        [
            {
                "root": "space1",
                "class": "onedatafs_jupyter.OnedataFSContentsManager",
                "config": {"space": u"/space1"},
            },
            {
                "root": "space2",
                "class": "onedatafs_jupyter.OnedataFSContentsManager",
                "config": {"space": u"/space2"},
            },
        ],
        help="""List of virtual mount point name and corresponding contents manager""",
        config=True,
    )

    def __init__(self, **kwargs):
        super(MixedContentsManager, self).__init__(**kwargs)
        self.managers = {}

        ## check consistency of scheme.
        if not len(set(map(lambda x: x["root"], self.filesystem_scheme))) == len(
            self.filesystem_scheme
        ):
            raise ValueError(
                "Scheme should not mount two contents manager on the same mountpoint"
            )

        kwargs.update({"parent": self})
        for scheme in self.filesystem_scheme:
            manager_class = import_item(scheme["class"])
            self.managers[scheme["root"]] = manager_class(**kwargs)
            if scheme["config"]:
                for k, v in scheme["config"].items():
                    setattr(self.managers[scheme["root"]], k, v)
        self.log.debug("MANAGERS: %s", self.managers)

    def _fix_paths(self, sub, base_path):
        self.log.debug("RETURNING: %s %s", sub, base_path)
        if type(sub) != dict:
            return sub
        if "path" in sub:
            sub["path"] = os.path.join(base_path, sub["path"])
        if sub.get("type") == "directory" and sub.get("content"):
            for e in sub.get("content"):
                if "path" in e:
                    e["path"] = os.path.join(base_path, e["path"].lstrip("/"))
        return sub

    def _get_cm(self, sentinel, listpath):
        self.log.debug("Finding cm for %s and %s", sentinel, listpath)
        if sentinel != self.mixed_path or not len(listpath):
            self.log.debug("Not found!")
            return None
        self.log.debug("FIND %s!", listpath[0])
        return self.managers.get(listpath[0], None)

    def path_dispatch1(method):
        def _wrapper_method(self, path, *args, **kwargs):
            sentinel, _path, path = _split_path(path)
            self.log.debug(
                "M: %s, S: %s, M: %s P: %s",
                method.__name__,
                sentinel,
                self.mixed_path,
                path,
            )
            man = self._get_cm(sentinel, _path)
            if man is not None:
                base_path = os.path.join(sentinel, _path[0])
                meth = getattr(man, method.__name__)
                sub = meth("/".join(_path[1:]), *args, **kwargs)
                return self._fix_paths(sub, base_path)
            else:
                return method(self, path, *args, **kwargs)

        return _wrapper_method

    def path_dispatch2(method):
        def _wrapper_method(self, other, path, *args, **kwargs):
            sentinel, _path, path = _split_path(path)
            self.log.debug(
                "M: %s, S: %s, M: %s P: %s",
                method.__name__,
                sentinel,
                self.mixed_path,
                path,
            )
            man = self._get_cm(sentinel, _path)
            if man is not None:
                base_path = os.path.join(sentinel, _path[0])
                meth = getattr(man, method.__name__)
                sub = meth(other, "/".join(_path[1:]), *args, **kwargs)
                return self._fix_paths(sub, base_path)
            else:
                return method(self, other, path, *args, **kwargs)

        return _wrapper_method

    def path_dispatch_kwarg(method):
        def _wrapper_method(self, path=""):
            sentinel, _path, path = _split_path(path)
            self.log.debug(
                "M: %s, S: %s, M: %s P: %s",
                method.__name__,
                sentinel,
                self.mixed_path,
                path,
            )
            man = self._get_cm(sentinel, _path)
            if man is not None:
                base_path = os.path.join(sentinel, _path[0])
                meth = getattr(man, method.__name__)
                sub = meth(path="/".join(_path[1:]))
                return self._fix_paths(sub, base_path)
            else:
                return method(self, path=path)

        return _wrapper_method

    # ContentsManager API part 1: methods that must be
    # implemented in subclasses.

    @path_dispatch1
    def dir_exists(self, path):
        ## root exists
        if (len(path) == 0) or path == self.mixed_path:
            return True
        return super(MixedContentsManager, self).dir_exists(os.path.join("/", path))

    @path_dispatch1
    def is_hidden(self, path):
        if (len(path) == 0) or path == self.mixed_path:
            return False
        return super(MixedContentsManager, self).is_hidden(os.path.join("/", path))

    @path_dispatch_kwarg
    def file_exists(self, path=""):
        if (len(path) == 0) or path == self.mixed_path:
            return False
        return super(MixedContentsManager, self).file_exists(os.path.join("/", path))

    @path_dispatch1
    def exists(self, path):
        if (len(path) == 0) or path == self.mixed_path:
            return True
        return super(MixedContentsManager, self).exists(os.path.join("/", path))

    @path_dispatch1
    def get(self, path, **kwargs):
        if len(path) == 0:
            root = super(MixedContentsManager, self).get("/", **kwargs)
            root["content"].append(
                {"type": "directory", "name": self.mixed_path, "path": self.mixed_path}
            )
            return root
        if path == self.mixed_path:
            root = {
                "type": "directory",
                "name": self.mixed_path,
                "path": self.mixed_path,
                "content": [],
                "last_modified": None,
                "created": None,
                "format": "json",
                "mimetype": None,
                "size": None,
                "writable": False,
                "type": "directory",
            }
            lm = []
            for subpath, manager in self.managers.items():
                try:
                    d = manager.get("/", **kwargs)
                    d["content"] = None
                    d["name"] = subpath
                    d["path"] = os.path.join(self.mixed_path, subpath)
                    root["content"].append(d)
                    lm.append(d["last_modified"])
                # TODO: have a better exception here
                except:
                    pass
            root["last_modified"] = max(lm)
            root["created"] = min(lm)
            return root
        return super(MixedContentsManager, self).get(os.path.join("/", path), **kwargs)

    @path_dispatch2
    def save(self, model, path):
        return super(MixedContentsManager, self).save(model, path)

    def update(self, model, path):
        sentinel, listpath, _path = _split_path(path)
        m_sentinel, m_listpath, orig_path = _split_path(model["path"])
        self.log.debug("UPDATE!")
        self.log.debug("s %s, l %s, p %s", sentinel, listpath, path)
        self.log.debug("s %s, l %s, p %s", m_sentinel, m_listpath, orig_path)
        man = self._get_cm(sentinel, listpath)
        m_man = self._get_cm(m_sentinel, m_listpath)
        if man != m_man:
            raise ValueError("Does not know how to move model across mountpoints")
        if man is not None:
            base_path = os.path.join(sentinel, listpath[0])
            model["path"] = "/".join(m_listpath[1:])
            meth = getattr(man, "update")
            sub = meth(model, "/".join(listpath[1:]))
            return self._fix_paths(sub, base_path)
        else:
            return super(MixedContentsManager, self).update(model, path)

    @path_dispatch1
    def delete(self, path):
        return super(MixedContentsManager, self).delete(path)

    @path_dispatch1
    def create_checkpoint(self, path):
        return super(MixedContentsManager, self).create_checkpoint(path)

    @path_dispatch1
    def list_checkpoints(self, path):
        return super(MixedContentsManager, self).list_checkpoints(path)

    @path_dispatch2
    def restore_checkpoint(self, checkpoint_id, path):
        return super(MixedContentsManager, self).restore_checkpoints(path)

    @path_dispatch2
    def delete_checkpoint(self, checkpoint_id, path):
        return super(MixedContentsManager, self).delete_checkpoints(path)

    # ContentsManager API part 2: methods that have useable default
    # implementations, but can be overridden in subclasses.

    # TODO (route optional methods too)

    # FIXME(enolfc) This is not yet reviewed
    ## Path dispatch on args 2 and 3 for rename.
    def path_dispatch_rename(rename_like_method):
        """
        decorator for rename-like function, that need dispatch on 2 arguments
        """

        def _wrapper_method(self, old_path, new_path):
            _, _old_path, old_sentinel = _split_path(old_path)
            _, _new_path, new_sentinel = _split_path(new_path)

            self.log.debug("%s AAAAAAAAAA %s", old_sentinel, new_sentinel)

            if old_sentinel != new_sentinel:
                raise ValueError(
                    "Does not know how to move things across contents manager mountpoints"
                )
            else:
                sentinel = new_sentinel

            man = self.managers.get(sentinel, None)
            if man is not None:
                rename_meth = getattr(man, rename_like_method.__name__)
                sub = rename_meth("/".join(_old_path), "/".join(_new_path))
                self.log.debug("AAAAAAAAAA")
                self.log.debug("AAAAAAAAAA")
                self.log.debug("AAAAAAAAAA")
                self.log.debug("AAAAAAAAAA")
                self.log.debug("AAAAAAAAAA")
                return self.fix_path(sentinel, sub)
            else:
                return rename_like_method(self, old_path, new_path)

        return _wrapper_method

    @path_dispatch_rename
    def rename_file(self, old_path, new_path):
        return super(MixedContentsManager, self).rename_file(old_path, new_path)

    @path_dispatch_rename
    def rename(self, old_path, new_path):
        """Rename a file."""
        return super(MixedContentsManager, self).rename(old_path, new_path)
