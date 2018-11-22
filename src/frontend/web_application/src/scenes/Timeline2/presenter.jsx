import React, { Component, Fragment } from 'react';
import PropTypes from 'prop-types';
import throttle from 'lodash.throttle';
import { Trans } from '@lingui/react';
import StickyNavBar from '../../layouts/Page/components/Navigation/components/StickyNavBar';
import { MessageNotifications } from '../../modules/notification';
import { Button, InfiniteScroll, Spinner } from '../../components';
import DiscussionItem from './components/DiscussionItem';
import { withSettings } from '../../modules/settings';

// XXX waiting for API
// import DiscussionSelector from './components/DiscussionSelector';

import './style.scss';

const LOAD_MORE_THROTTLE = 1000;

@withSettings()
class Timeline extends Component {
  static propTypes = {
    user: PropTypes.shape({}),
    getUser: PropTypes.func.isRequired,
    isUserFetching: PropTypes.bool.isRequired,
    requestDiscussions: PropTypes.func.isRequired,
    // XXX no API for now
    // deleteDiscussion: PropTypes.func.isRequired,
    loadMore: PropTypes.func.isRequired,
    discussions: PropTypes.arrayOf(PropTypes.shape({})),
    // tags: PropTypes.arrayOf(PropTypes.shape({})),
    isFetching: PropTypes.bool,
    // didInvalidate: PropTypes.bool,
    hasMore: PropTypes.bool,
    // updateDiscussionTags: PropTypes.func.isRequired,
    settings: PropTypes.shape({}).isRequired,
  };

  static defaultProps = {
    discussions: [],
    // tags: [],
    user: {},
    isFetching: false,
    // didInvalidate: false,
    hasMore: false,
  };

  state = {
    initialized: false,
    selectedDiscussions: [],
    // XXX no API for now
    isTagModalOpen: false,
    // isDeleting: false,
  };

  componentDidMount() {
    const { user, isUserFetching, getUser } = this.props;

    if (!(user || isUserFetching)) {
      getUser();
    }

    this.loadDiscussions(this.props);

    this.throttledLoadMore = throttle(
      () => this.props.loadMore(),
      LOAD_MORE_THROTTLE,
      { trailing: false }
    );
  }

  componentWillReceiveProps(nextProps) {
    this.loadDiscussions(nextProps);
  }

  onSelectDiscussion = (type, discussionId) => {
    if (type === 'add') {
      this.setState(prevState => ({
        ...prevState,
        selectedDiscussions: [...prevState.selectedDiscussions, discussionId],
      }));
    }

    if (type === 'remove') {
      this.setState(prevState => ({
        ...prevState,
        selectedDiscussions: [...prevState.selectedDiscussions]
          .filter(item => item !== discussionId),
      }));
    }
  };

  onSelectAllDiscussions = (type) => {
    if (type === 'select') {
      const { discussions } = this.props;

      this.setState(prevState =>
        ({
          ...prevState,
          selectedDiscussions:
            discussions.map(discussion => discussion.discussion_id),
        }));
    }

    if (type === 'unselect') {
      this.setState(prevState => ({ ...prevState, selectedDiscussions: [] }));
    }
  };

  loadDiscussions = async (props, force = false) => {
    const {
      requestDiscussions, isFetching,
    } = props;
    if ((!this.state.initialized || force) && !isFetching) {
      // "initialized" is not well named,
      // we consider it "initialized" as soon as we start fetching messages to prevent multiple
      // fetchs because setState would be applied at the very end after multiple
      // componentWillReceiveProps
      this.setState({ initialized: true });
      requestDiscussions();
    }

    return Promise.resolve();
  }

  loadMore = () => {
    if (this.props.hasMore) {
      this.throttledLoadMore();
    }
  };

  makeHandleClickClearNotifications = cb => () => {
    this.loadDiscussions(this.props, true);
    cb();
  }

  renderNotifications = () => (
    <MessageNotifications
      key="1"
      render={({ notifications, clearNotifications }) => {
        if (!notifications.length) {
          return null;
        }

        return (
          <div className="s-timeline__new-msg">
            <Button display="inline" onClick={this.makeHandleClickClearNotifications(clearNotifications)}>
              <Trans id="timeline.new_messages" values={[notifications.length]}>You have {0} new messages</Trans>
            </Button>
          </div>
        );
      }}
    />
  )

  renderDiscussions() {
    const { discussions, user, settings } = this.props;
    const { selectedDiscussions } = this.state;

    if (discussions) {
      return (
        <ul className="s-discussion-list">
          {discussions.map(discussion => (
            <DiscussionItem
              key={discussion.discussion_id}
              user={user}
              discussion={discussion}
              onSelectDiscussion={this.onSelectDiscussion}
              onSelectAllDiscussions={this.onSelectAllDiscussions}
              isDiscussionSelected={selectedDiscussions.includes(discussion.discussion_id)}
              settings={settings}
            />
          ))}
        </ul>);
    }

    return (<Spinner isLoading />);
  }

  render() {
    const { discussions, hasMore } = this.props;
    // const nbSelectedDiscussions = this.state.selectedDiscussions.length;

    return (
      <Fragment>
        <section id="discussions" className="s-timeline">
          <StickyNavBar className="s-timeline__action-bar" stickyClassName="sticky">
            <div className="s-timeline__actions--placeholder">
              {discussions ? null : <Spinner isLoading /> }
            </div>
            {/*  <DiscussionSelector
              count={nbSelectedDiscussions}
              checked={nbSelectedDiscussions > 0
                  && nbSelectedDiscussions === discussions.length}
                  totalCount={discussions.length}
                  onSelectAllDiscussions={this.onSelectAllDiscussions}
                  onEditTags={this.handleOpenTags}
                  onDeleteDiscussions={this.handleDeleteDiscussions}
                  isDeleting={this.state.isDeleting}
                  indeterminate={nbSelectedDiscussions > 0
                      && nbSelectedDiscussions < discussions.length}
                    />
                    */
            }
          </StickyNavBar>
          <InfiniteScroll onReachBottom={this.loadMore}>
            <Fragment>
              {this.renderNotifications()}
              {this.renderDiscussions()}
            </Fragment>
          </InfiniteScroll>
        </section>
        {hasMore && (
          <div className="s-timeline__load-more">
            <Button shape="hollow" onClick={this.loadMore}><Trans id="general.action.load_more">Load more</Trans></Button>
          </div>
        )}
      </Fragment>
    );
  }
}

export default Timeline;