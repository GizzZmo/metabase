import cx from "classnames";
import { memo } from "react";

import Card from "metabase/common/components/Card";
import { Ellipsified } from "metabase/common/components/Ellipsified";
import S from "metabase/common/components/List/List.module.css";
import CS from "metabase/css/core/index.css";
import { Icon } from "metabase/ui";

import { ListItemLink, Root } from "./ListItem.styled";

interface ListItemProps {
  "data-testid"?: string;
  /** The display name of the list item */
  name: string;
  /** The URL to navigate to when the item is clicked */
  url?: string;
  /** Optional description text to display below the name */
  description?: string;
  /** Whether the item should be disabled (non-interactive) */
  disabled?: boolean;
  /** Placeholder text to show when no description is provided */
  placeholder?: string;
  /** Optional icon name to display next to the item */
  icon?: string;
}

const ListItem = ({
  "data-testid": dataTestId,
  name,
  url,
  description,
  disabled,
  placeholder,
  icon,
}: ListItemProps) => {
  const card = (
    <Card
      hoverable
      className={cx(CS.mb2, CS.p3, CS.bgWhite, CS.rounded, CS.bordered)}
      data-testid="data-reference-list-item"
      style={{ width: "680px" }}
    >
      <div className={cx(S.item)}>
        <div className={S.itemIcons}>
          {icon && <Icon className={S.chartIcon} name={icon} size={16} />}
        </div>
        <div className={cx(S.itemBody, CS.flexColumn)}>
          <div className={S.itemTitle}>
            <Ellipsified tooltip={name}>{name}</Ellipsified>
          </div>
          {(description || placeholder) && (
            <div className={cx(S.itemSubtitle)}>
              {description || placeholder}
            </div>
          )}
        </div>
      </div>
    </Card>
  );

  if (disabled) {
    return (
      <Root data-testid={dataTestId} disabled>
        {card}
      </Root>
    );
  }

  return (
    <Root data-testid={dataTestId}>
      <ListItemLink to={url}>{card}</ListItemLink>
    </Root>
  );
};

export default memo(ListItem);
