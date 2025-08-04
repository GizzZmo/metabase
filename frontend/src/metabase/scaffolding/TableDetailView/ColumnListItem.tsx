import { t } from "ttag";

import {
  ActionIcon,
  Box,
  Flex,
  Group,
  Icon,
  type IconName,
  Text,
  Tooltip,
} from "metabase/ui/components";
import { getIconForField } from "metabase-lib/v1/metadata/utils/fields";
import type { DatasetColumn } from "metabase-types/api";

import { ColumnFormatControl } from "../components/ColumnFormatControl";

import S from "./TableDetailView.module.css";

interface Props {
  draggable?: boolean;
  handleProps: any;
  column: DatasetColumn;
  style?: "normal" | "bold" | "dim" | "title";
  onChangeFieldSettings?: (update: {
    style: "normal" | "bold" | "dim" | "title";
  }) => void;
  onHideField?: () => void;
  onUnhideField?: () => void;
}

export function ColumnListItem({
  draggable = true,
  handleProps,
  column,
  style,
  onChangeFieldSettings,
  onHideField,
  onUnhideField,
}: Props) {
  return (
    <Box
      className={S.ObjectViewSidebarColumn}
      w="100%"
      // style={{
      //   transition,
      //   transform: CSS.Transform.toString(transform),
      //   opacity: isDragging ? 0.5 : 1,
      // }}
      // ref={setNodeRef}
    >
      <Flex pl="md" align="center" justify="space-between">
        <Group gap="sm">
          {draggable && (
            <Icon
              name="grabber"
              className={S.ObjectViewSidebarColumnActionIcon}
              style={{ cursor: "grab" }}
              {...handleProps}
              // {...attributes}
              // {...listeners}
            />
          )}
          <Icon name={getIconForField(column) as IconName} />
          <Text>{column.display_name}</Text>
        </Group>
        <Group gap="sm">
          {!!onHideField && (
            <Tooltip label={t`Hide column`}>
              <ActionIcon
                className={S.ObjectViewSidebarColumnActionIcon}
                variant="transparent"
                color="text-medium"
                onClick={onHideField}
              >
                <Icon name="eye" />
              </ActionIcon>
            </Tooltip>
          )}
          {!!onUnhideField && (
            <Tooltip label={t`Unhide column`}>
              <ActionIcon
                className={S.ObjectViewSidebarColumnActionIcon}
                variant="transparent"
                color="text-medium"
                onClick={onUnhideField}
              >
                <Icon name="eye_crossed_out" />
              </ActionIcon>
            </Tooltip>
          )}
          {!!onChangeFieldSettings && (
            <ColumnFormatControl
              style={style}
              onStyleChange={(style) => onChangeFieldSettings({ style })}
              actionIconProps={{
                className: S.ObjectViewSidebarColumnActionIcon,
              }}
            />
          )}
        </Group>
      </Flex>
    </Box>
  );
}
