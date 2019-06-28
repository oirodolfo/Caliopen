# -*- coding: utf-8 -*-
"""Caliopen device core classes."""
from __future__ import absolute_import, print_function, unicode_literals

from caliopen_storage.core import BaseCore

from ..store.provider import Provider as ModelProvider


class Provider(BaseCore):
    """Provider core class"""

    _model_class = ModelProvider
    _pkey_name = "name"
