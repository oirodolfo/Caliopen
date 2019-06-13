# -*- coding: utf-8 -*-
"""Caliopen inbound nats message handler."""
from __future__ import absolute_import, print_function, unicode_literals

import logging
import json
import sys
import traceback

from caliopen_storage.exception import DuplicateObject
from caliopen_main.user.core import User, UserIdentity
from caliopen_main.contact.objects import Contact

from caliopen_nats.delivery import UserMailDelivery, UserTwitterDMDelivery
from caliopen_pi.qualifiers import ContactMessageQualifier
from caliopen_pgp.keys import ContactPublicKeyManager
from caliopen_main.common.core import PublicKey

log = logging.getLogger(__name__)


class BaseHandler(object):
    """Base class for NATS message handlers."""

    def __init__(self, nats_cnx):
        """Create a new inbound messsage handler from a nats connection."""
        self.natsConn = nats_cnx


class InboundEmail(BaseHandler):
    """Inbound message class handler."""

    def process_raw(self, msg, payload):
        """Process an inbound raw message."""
        nats_error = {
            'error': '',
            'message': 'inbound email message process failed'
        }
        nats_success = {
            'message': 'OK : inbound email message proceeded'
        }
        try:
            user = User.get(payload['user_id'])
            identity = UserIdentity.get(user, payload['identity_id'])
            deliver = UserMailDelivery(user, identity)
            new_message = deliver.process_raw(payload['message_id'])
            nats_success['message_id'] = str(new_message.message_id)
            nats_success['discussion_id'] = str(new_message.discussion_id)
            self.natsConn.publish(msg.reply, json.dumps(nats_success))
        except DuplicateObject:
            log.info("Message already imported : {}".format(payload))
            nats_success['message_id'] = str(payload['message_id'])
            nats_success['discussion_id'] = ""  # message has not been parsed
            nats_success['message'] = 'raw message already imported'
            self.natsConn.publish(msg.reply, json.dumps(nats_success))
        except Exception as exc:
            # TODO: handle abort exception and report it as special case
            exc_info = sys.exc_info()
            log.error("deliver process failed for raw {}: {}".
                      format(payload, traceback.print_exception(*exc_info)))
            nats_error['error'] = str(exc)
            self.natsConn.publish(msg.reply, json.dumps(nats_error))
            return exc

    def handler(self, msg):
        """Handle an process_raw nats messages."""
        payload = json.loads(msg.data)
        log.info('Get payload order {}'.format(payload))
        if payload['order'] == "process_raw":
            self.process_raw(msg, payload)
        else:
            log.warn(
                'Unhandled payload order "{}" \
                (queue: SMTPqueue, subject : inboundSMTP)'.format(
                    payload['order']))
            raise NotImplementedError


class InboundTwitter(BaseHandler):
    """Inbound TwitterDM class handler."""

    def process_raw(self, msg, payload):
        """Process an inbound raw message."""
        nats_error = {
            'error': '',
            'message': 'inbound twitter message process failed'
        }
        nats_success = {
            'message': 'OK : inbound twitter message proceeded'
        }
        try:
            user = User.get(payload['user_id'])
            identity = UserIdentity.get(user, payload['identity_id'])
            deliver = UserTwitterDMDelivery(user, identity)
            new_message = deliver.process_raw(payload['message_id'])
            nats_success['message_id'] = str(new_message.message_id)
            nats_success['discussion_id'] = str(new_message.discussion_id)
            self.natsConn.publish(msg.reply, json.dumps(nats_success))
        except DuplicateObject:
            log.info("Message already imported : {}".format(payload))
            nats_success['message_id'] = str(payload['message_id'])
            nats_success['discussion_id'] = ""  # message has not been parsed
            nats_success['message'] = 'raw message already imported'
            self.natsConn.publish(msg.reply, json.dumps(nats_success))
        except Exception as exc:
            # TODO: handle abort exception and report it as special case
            exc_info = sys.exc_info()
            log.error("deliver process failed for raw {}: {}".
                      format(payload, traceback.print_exception(*exc_info)))
            nats_error['error'] = str(exc.message)
            self.natsConn.publish(msg.reply, json.dumps(nats_error))
            return exc

    def handler(self, msg):
        """Handle an process_raw nats messages."""
        payload = json.loads(msg.data)
        log.info('Get payload order {}'.format(payload['order']))
        if payload['order'] == "process_raw":
            self.process_raw(msg, payload)
        else:
            log.warn('Unhandled payload type {}'.format(payload['order']))


class ContactAction(BaseHandler):
    """Handler for contact action message."""

    def process_update(self, msg, payload):
        """Process a contact update message."""
        # XXX validate payload structure
        if 'user_id' not in payload or 'contact_id' not in payload:
            raise Exception('Invalid contact_update structure')
        user = User.get(payload['user_id'])
        contact = Contact(user, contact_id=payload['contact_id'])
        contact.get_db()
        contact.unmarshall_db()
        qualifier = ContactMessageQualifier(user)
        log.info('Will process update for contact {0} of user {1}'.
                 format(contact.contact_id, user.user_id))
        # TODO: (re)discover GPG keys
        qualifier.process(contact)

    def handler(self, msg):
        """Handle an process_raw nats messages."""
        payload = json.loads(msg.data)
        # log.info('Get payload order {}'.format(payload['order']))
        if payload['order'] == "contact_update":
            self.process_update(msg, payload)
        else:
            log.warn(
                'Unhandled payload order "{}" \
                (queue: contactQueue, subject : contactAction)'.format(
                    payload['order']))
            raise NotImplementedError


class KeyAction(BaseHandler):
    """Handler for public key discovery message."""

    def _process_key(self, user, contact, key):
        if not key.is_expired:
            if key.userids:
                label = key.userids[0].name
            else:
                label = '{0} {1}'.format(key.algorithm, key.size)
            pub = PublicKey.create(user, contact.contact_id, 'contact',
                                   fingerprint=key.fingerprint,
                                   key=key.armored_key,
                                   expire_date=key.expire_date,
                                   label=label)
            log.info('Created public key {0}'.format(pub.key_id))

    def _process_results(self, user, contact, results):
        fingerprints = []
        for result in results:
            for key in result.keys:
                log.debug('Processing key %r' % key)
                if key.fingerprint not in fingerprints:
                    self._process_key(user, contact, key)
                    fingerprints.append(key.fingerprint)

    def process_key_discovery(self, msg, payload):
        """Discover public keys related to a new contact identifier."""
        if 'user_id' not in payload or 'contact_id' not in payload:
            raise Exception('Invalid contact_update structure')
        user = User.get(payload['user_id'])
        contact = Contact(user.user_id, contact_id=payload['contact_id'])
        contact.get_db()
        contact.unmarshall_db()
        manager = ContactPublicKeyManager()
        founds = []
        for ident in payload.get('emails', []):
            log.info('Process email identity {0}'.format(ident['address']))
            discovery = manager.process_identity(user, contact,
                                                 ident['address'], 'email')
            if discovery:
                founds.append(discovery)
        for ident in payload.get('identities', []):
            log.info('Process identity {0}:{1}'.
                     format(ident['type'], ident['name']))
            discovery = manager.process_identity(user, contact,
                                                 ident['name'], ident['type'])
            if discovery:
                founds.append(discovery)
        if founds:
            log.info('Found %d results' % len(founds))
            self._process_results(user, contact, founds)

    def handler(self, msg):
        """Handle a discover_key nats messages."""
        payload = json.loads(msg.data)
        if payload['order'] == "discover_key":
            self.process_key_discovery(msg, payload)
        else:
            log.warn(
                'Unhandled payload order "{}" \
                (queue : keyQueue, subject : keyAction)'.format(
                    payload['order']))
            raise NotImplementedError
