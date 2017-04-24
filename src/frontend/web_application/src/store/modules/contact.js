// import calcObjectForPatch from '../../services/api-patch';
export const REQUEST_CONTACTS = 'co/contact/REQUEST_CONTACTS';
export const REQUEST_CONTACTS_SUCCESS = 'co/contact/REQUEST_CONTACTS_SUCCESS';
export const REQUEST_CONTACTS_FAIL = 'co/contact/REQUEST_CONTACTS_FAIL';
export const INVALIDATE_CONTACTS = 'co/contact/INVALIDATE_CONTACTS';
export const LOAD_MORE_CONTACTS = 'co/contact/LOAD_MORE_CONTACTS';
export const REQUEST_CONTACT = 'co/contact/REQUEST_CONTACT';
export const REMOVE_CONTACT = 'co/contact/REMOVE_CONTACT';

export function requestContacts(params = {}) {
  const { offset = 0, limit = 1000 } = params;

  return {
    type: REQUEST_CONTACTS,
    payload: {
      request: {
        url: '/v1/contacts',
        params: { offset, limit },
      },
    },
  };
}

export function loadMoreContacts() {
  return {
    type: LOAD_MORE_CONTACTS,
    payload: {},
  };
}

export function requestContact({ contactId }) {
  return {
    type: REQUEST_CONTACT,
    payload: {
      request: {
        url: `/v1/contacts/${contactId}`,
      },
    },
  };
}

export function invalidate() {
  return {
    type: INVALIDATE_CONTACTS,
    payload: {},
  };
}

function contactsByIdReducer(state = {}, action = {}) {
  return action.payload.data.contacts.reduce((previousState, contact) => ({
    ...previousState,
    [contact.contact_id]: contact,
  }), state);
}

function contactListReducer(state = [], action = {}) {
  if (action.type !== REQUEST_CONTACTS_SUCCESS) {
    return state;
  }

  return [...state]
    .concat(action.payload.data.contacts.map(contact => contact.contact_id))
    .reduce((prev, curr) => {
      if (prev.indexOf(curr) === -1) {
        prev.push(curr);
      }

      return prev;
    }, []);
}

export function getNextOffset(state) {
  return state.contacts.length;
}

export function hasMore(state) {
  return state.total > state.contacts.length;
}

const initialState = {
  isFetching: false,
  didInvalidate: false,
  contactsById: {},
  contacts: [],
  total: 0,
};

export default function reducer(state = initialState, action) {
  switch (action.type) {
    case REQUEST_CONTACTS:
      return { ...state, isFetching: true };
    case REQUEST_CONTACTS_SUCCESS:
      return {
        ...state,
        isFetching: false,
        didInvalidate: false,
        contacts: contactListReducer(
          state.didInvalidate === true ? [] : state.contacts,
          action
        ),
        contactsById: contactsByIdReducer(
          state.didInvalidate === true ? {} : state.contactsById,
          action
        ),
        total: action.payload.data.total,
      };
    case INVALIDATE_CONTACTS:
      return { ...state, didInvalidate: true };
    default:
      return state;
  }
}
