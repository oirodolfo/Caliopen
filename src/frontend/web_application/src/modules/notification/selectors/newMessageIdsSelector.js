import { createSelector } from 'reselect';
import { getModuleStateSelector } from '../../../store/selectors/getModuleStateSelector';
import { messageNotificationsSelector } from './messageNotificationsSelector';

export const newMessageIdsSelector = createSelector(
  [messageNotificationsSelector, getModuleStateSelector('message')],
  (notifications, { messagesById }) => {
    const currMessagesIds = Object.keys(messagesById);

    return notifications.map((notif) => {
      const body = JSON.parse(notif.body);

      switch (notif.emitter) {
        case 'smtp':
          return body.emailReceived;
        case 'mastodonBroker':
        case 'twitterBroker':
          return body.dmReceived;
        default:
          throw new Error(`unknown emitter ${notif.emitter}`);
      }
    }).filter(messageId => !currMessagesIds.includes(messageId));
  }
);
