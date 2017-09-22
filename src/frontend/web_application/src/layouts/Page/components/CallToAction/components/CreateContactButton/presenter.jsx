import React, { PureComponent } from 'react';
import PropTypes from 'prop-types';
import ActionButton from '../ActionButton';

class ComposeContactButton extends PureComponent {
  static propTypes = {
    __: PropTypes.func.isRequired,
    notify: PropTypes.func.isRequired,
    className: PropTypes.string,
  };
  static defaultProps = {
    className: undefined,
  };

  handleEventAction = () => {
    this.props.notify({
      message: 'Contact creation not yet implemented.',
    });
  }

  render() {
    const { __, className } = this.props;
    const buttonProps = {
      icon: 'user',
      className,
    };

    return (
      <ActionButton action={this.handleEventAction} button={buttonProps}>
        {__('call-to-action.action.create_contact')}
      </ActionButton>
    );
  }
}

export default ComposeContactButton;
