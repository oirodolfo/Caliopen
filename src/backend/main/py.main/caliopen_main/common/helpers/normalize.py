# -*- coding: utf-8 -*-
"""Normalization functions for different values."""
from __future__ import absolute_import, unicode_literals
import re
import logging
from email.utils import parseaddr

log = logging.getLogger(__name__)

mastodon_url_regex = '^https:\/\/(.*)\/@(.*)'
mastodon_url_legacy_regex = '^https:\/\/(.*)\/users\/(.*)'


def clean_email_address(addr):
    """Clean an email address for user resolve."""
    try:
        real_name, email = parseaddr(addr.replace('\r', ''))
    except UnicodeError:
        addr = addr.decode('utf-8', errors='ignore')
        real_name, email = parseaddr(addr.replace('\r', ''))
    err_msg = 'Invalid email address {}'.format(addr)
    if not email or '@' not in email:
        # Try something else
        log.info('Last chance email parsing for {}'.format(addr))
        matches = re.match('(.*)<(.*@.*)>', addr)
        if matches and matches.groups():
            real_name, email = matches.groups()
        else:
            log.warn(err_msg)
            return ("", "")
    name, domain = email.lower().split('@', 1)
    if '@' in domain:
        log.error(err_msg)
        return ("", "")
    if '+' in name:
        try:
            name, ext = name.split('+', 1)
        except Exception as exc:
            log.info(exc)
    # unicode everywhere
    return (u'%s@%s' % (name, domain), email)


def clean_twitter_address(addr):
    return addr.strip('@').lower()


def clean_mastodon_address(addr):
    return addr.strip('@').lower().split('@')


def parse_mastodon_url(url):
    """extract username and domain from a mastodon account url
    in the format https://instance.tld/@username

    :return: tuple (server, username)
    """

    matches = re.findall(mastodon_url_regex, url)
    if len(matches) != 1 or len(matches[0]) != 2:
        # try legacy fallback
        matches = re.findall(mastodon_url_legacy_regex, url)
        if len(matches) != 1 or len(matches[0]) != 2:
            raise SyntaxError

    return matches[0]
