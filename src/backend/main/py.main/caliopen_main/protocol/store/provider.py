# -*- coding: utf-8 -*-
"""Caliopen tag objects."""
from __future__ import absolute_import, print_function, unicode_literals

from cassandra.cqlengine import columns
from caliopen_storage.store.model import BaseModel


class Provider(BaseModel):
    """model to store data related to external protocol endpoints"""

    name = columns.Text(primary_key=True)
    instance = columns.Text(primary_key=True)
    infos = columns.Map(columns.Text, columns.Text)
    date_insert = columns.DateTime()
