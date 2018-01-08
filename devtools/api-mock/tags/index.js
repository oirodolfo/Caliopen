import { createAction, createSelector } from 'bouchon';
import { v1 as uuidv1 } from 'uuid';
import createCollectionMiddleware from '../collection-middleware';

const actions = {
  get: createAction('Get tags'),
  post: createAction('Create tag'),
  delete: createAction('Delete a tag'),
};

const selectors = {
  all: () => state => state.tags,
  last: () => state => [...state.tags].pop(),
  byName: ({ name }) => createSelector(
    selectors.all(),
    tags => tags.find(tag => tag.name === name)
  ),
  location: () => createSelector(
    selectors.last(),
    tag => ({ location: `/api/v2/tags/${tag.name}` })
  ),
};

const reducer = {
  [actions.get]: state => state,
  [actions.post]: (state, params) => ([
    ...state,
    { ...params.body, type: 'user', label: params.body.label || params.body.name },
  ]),
  [actions.delete]: (state, { params }) => {
    const copy = state.slice(0);
    return [...state].filter(tag => tag.name !== params.name);
  },
};

const routes = {
  'GET /': {
    action: actions.get,
    selector: selectors.all,
    status: 200,
    middlewares: [createCollectionMiddleware('tags')],
  },
  'POST /': {
    action: actions.post,
    selector: selectors.location,
    status: 200,
  },
  'GET /:name': {
    action: actions.get,
    selector: selectors.byName,
    status: 200,
  },
  'DELETE /:name': {
    action: actions.delete,
    status: 204,
  },
};

export default {
  name: 'tags',
  data: require('./data.json'),
  reducer: reducer,
  endpoint: '/api/v2/tags',
  routes: routes,
};
