import { t } from "ttag";

import { GroupsListingApp } from "metabase/admin/people/containers/GroupsListingApp";

export const ExternalGroupsListingApp = () => {
  return (
    <GroupsListingApp
      title={t`Tenant Groups`}
      description={t`Use tenant groups to manage access for external users. Every tenant has access to these groups.`}
      external
    />
  );
};
