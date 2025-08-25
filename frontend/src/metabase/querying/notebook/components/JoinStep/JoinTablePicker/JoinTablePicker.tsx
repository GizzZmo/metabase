import { t } from "ttag";

import IconButtonWrapper from "metabase/common/components/IconButtonWrapper";
import { Icon, Popover, Tooltip } from "metabase/ui";
import type * as Lib from "metabase-lib";

import { NotebookCellItem } from "../../NotebookCell";
import { CONTAINER_PADDING } from "../../NotebookCell/constants";
import { NotebookDataPicker } from "../../NotebookDataPicker";

import S from "./JoinTablePicker.module.css";

interface JoinTablePickerProps {
  query: Lib.Query;
  stageIndex: number;
  table: Lib.Joinable | undefined;
  color: string;
  isReadOnly: boolean;
  columnPicker: React.ReactNode;
  onChange: (table: Lib.Joinable) => void;
  isOpened: boolean;
  setIsOpened: (isOpened: boolean) => void;
}

export function JoinTablePicker({
  query,
  stageIndex,
  table,
  color,
  isReadOnly,
  columnPicker,
  onChange,
  isOpened,
  setIsOpened,
}: JoinTablePickerProps) {
  const isDisabled = isReadOnly;

  return (
    <NotebookCellItem
      inactive={!table}
      readOnly={isReadOnly}
      disabled={isDisabled}
      color={color}
      right={
        table != null && !isReadOnly ? (
          <JoinTableColumnPickerWrapper
            columnPicker={columnPicker}
            isOpened={isOpened}
            setIsOpened={setIsOpened}
          />
        ) : null
      }
      containerStyle={CONTAINER_STYLE}
      rightContainerStyle={RIGHT_CONTAINER_STYLE}
      aria-label={t`Right table`}
    >
      <NotebookDataPicker
        title={t`Pick data to join`}
        query={query}
        stageIndex={stageIndex}
        table={table}
        placeholder={t`Pick data…`}
        canChangeDatabase={false}
        hasMetrics={false}
        isDisabled={isDisabled}
        onChange={onChange}
      />
    </NotebookCellItem>
  );
}

interface JoinTableColumnPickerProps {
  columnPicker: React.ReactNode;
  isOpened: boolean;
  setIsOpened: (isOpened: boolean) => void;
}

function JoinTableColumnPickerWrapper({
  columnPicker,
  isOpened,
  setIsOpened,
}: JoinTableColumnPickerProps) {
  return (
    <Popover
      opened={isOpened}
      onChange={setIsOpened}
      withOverlay={false}
      closeOnClickOutside={false}
    >
      <Popover.Target>
        <Tooltip label={t`Pick columns`}>
          <IconButtonWrapper
            className={S.ColumnPickerButton}
            style={
              {
                "--notebook-cell-container-padding": CONTAINER_PADDING,
              } as React.CSSProperties
            }
            onClick={() => setIsOpened(!isOpened)}
            aria-label={t`Pick columns`}
            data-testid="fields-picker"
          >
            <Icon name="notebook" />
          </IconButtonWrapper>
        </Tooltip>
      </Popover.Target>
      <Popover.Dropdown>{columnPicker}</Popover.Dropdown>
    </Popover>
  );
}

const CONTAINER_STYLE = {
  padding: 0,
};

const RIGHT_CONTAINER_STYLE = {
  width: 37,
  height: "100%",
  padding: 0,
};
