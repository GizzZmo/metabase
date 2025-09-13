import type { HTMLAttributes, ReactNode } from "react";

import { GridItemRoot, GridRoot } from "./Grid.styled";

interface GridProps extends HTMLAttributes<HTMLDivElement> {
  /** Child elements to be rendered in the grid */
  children?: ReactNode;
}

interface GridItemProps extends HTMLAttributes<HTMLDivElement> {
  /** Child elements to be rendered in the grid item */
  children?: ReactNode;
}

/**
 * A flexible grid container component that displays children in a responsive layout.
 * Uses CSS flexbox with wrapping to create a 4-column grid by default.
 */
export const Grid = ({ children, ...props }: GridProps) => (
  <GridRoot {...props}>
    {children}
  </GridRoot>
);

/**
 * Individual grid item component for use within the Grid component.
 * Takes up 25% width by default (4 columns) with consistent padding.
 */
export const GridItem = ({ children, ...props }: GridItemProps) => (
  <GridItemRoot {...props}>
    {children}
  </GridItemRoot>
);
