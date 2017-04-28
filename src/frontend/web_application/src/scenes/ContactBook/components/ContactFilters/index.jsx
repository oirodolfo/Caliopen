import React from 'react';
import PropTypes from 'prop-types';
import { FormGrid, FormRow, FormColumn, SelectFieldGroup } from '../../../../components/form';

import './style.scss';

const ContactFilters = ({ onSortDirChange, onSortViewChange, sortView, sortDir, __ }) => (
  <FormGrid className="m-contacts-filters">
    <FormRow>
      <FormColumn size="shrink">
        <SelectFieldGroup
          name="format-view"
          className="m-contacts-filters__select"
          label={__('contacts-filters.format-view.label')}
          value={sortView}
          options={[
            { value: 'given_name', label: __('contacts-filters.format-view.firstname') },
            { value: 'family_name', label: __('contacts-filters.format-view.name') },
          ]}
          onChange={onSortViewChange}
        />
      </FormColumn>
      <FormColumn size="shrink">
        <SelectFieldGroup
          className="m-contacts-filters__select"
          name="order-by"
          label={__('contacts-filters.order-by.label')}
          value={sortDir}
          options={[{ value: 'ASC', label: 'A -> Z' }, { value: 'DESC', label: 'Z -> A' }]}
          onChange={onSortDirChange}
        />
      </FormColumn>
    </FormRow>
  </FormGrid>
);

ContactFilters.propTypes = {
  onSortDirChange: PropTypes.func.isRequired,
  onSortViewChange: PropTypes.func.isRequired,
  __: PropTypes.func.isRequired,
  sortDir: PropTypes.oneOf('ASC', 'DESC').isRequired,
  sortView: PropTypes.oneOf('given_name', 'family_name').isRequired,
};

export default ContactFilters;
