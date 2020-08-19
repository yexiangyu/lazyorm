import json
import yaml
import os
from argparse import ArgumentParser


class LConfig(object):
    """get config item with convinience

    """

    __seqs = ['args', 'env', 'file']

    def __init__(self, parser=None, file_name=None, seq=None):
        """ create LDConfig instnace

        Args:
            parser (argparse.ArgumentParser, optional): parser to get cli input. Defaults to None.
            file_name (str, optional): json/yaml file store config. Defaults to None.
            seq (list, optional): search sequence from args, env and json/yaml. Defaults to None.
        """

        self._parser = parser
        self._file_name = file_name
        self._seq = self.__seqs if seq is None or not isinstance(seq, list) else [s for s in seq if s in self.__seqs]

        self.stores = dict(
            args=self.init_args(),
            env=os.environ,
            file=self.init_file()
        )

    def init_args(self):
        if self._parser is None:
            return {}

        assert isinstance(self._parser, ArgumentParser)

        return self._parser.parse_args()

    def init_file(self):
        if self._file_name is None:
            return {}

        assert os.path.isfile(self._file_name)

        content = open(self._file_name, 'r').read()

        try:
            return json.loads(content)
        except Exception:
            return yaml.load(content)

    @property
    def seq(self):
        return self._seq

    @seq.setter
    def seq(self, _seq):
        assert _seq is None or not isinstance(_seq, list)
        self._seq = [s for s in _seq if s in self.__seqs]
        assert self._seq

    def get(self, *keys, def_val=None, def_type=None):
        assert len(keys)

        for key in keys:
            for key_type in self.seq:
                if key in self.stores[key_type]:

                    if key_type == 'args':
                        v = getattr(self.stores[key_type], key, None)
                    else:
                        v = self.stores[key_type].get(key, None)

                    if v is not None:
                        return def_type(v) if def_type else v

        return def_type(def_val) if def_type is not None and def_val is not None else def_val
