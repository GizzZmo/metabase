import { render, screen } from "@testing-library/react";

import { Grid, GridItem } from "./Grid";

describe("Grid", () => {
  it("should render grid container with children", () => {
    render(
      <Grid data-testid="grid-container">
        <div>Child 1</div>
        <div>Child 2</div>
      </Grid>
    );

    const gridContainer = screen.getByTestId("grid-container");
    expect(gridContainer).toBeInTheDocument();
    expect(screen.getByText("Child 1")).toBeInTheDocument();
    expect(screen.getByText("Child 2")).toBeInTheDocument();
  });

  it("should pass through HTML attributes to grid container", () => {
    render(
      <Grid 
        data-testid="grid-container" 
        className="custom-class"
        role="grid"
      >
        Test content
      </Grid>
    );

    const gridContainer = screen.getByTestId("grid-container");
    expect(gridContainer).toHaveClass("custom-class");
    expect(gridContainer).toHaveAttribute("role", "grid");
  });
});

describe("GridItem", () => {
  it("should render grid item with children", () => {
    render(
      <GridItem data-testid="grid-item">
        <span>Item content</span>
      </GridItem>
    );

    const gridItem = screen.getByTestId("grid-item");
    expect(gridItem).toBeInTheDocument();
    expect(screen.getByText("Item content")).toBeInTheDocument();
  });

  it("should pass through HTML attributes to grid item", () => {
    render(
      <GridItem 
        data-testid="grid-item" 
        className="item-class"
        onClick={() => {}}
      >
        Clickable item
      </GridItem>
    );

    const gridItem = screen.getByTestId("grid-item");
    expect(gridItem).toHaveClass("item-class");
  });
});