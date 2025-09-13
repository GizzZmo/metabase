import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route } from "react-router";

import { renderWithProviders } from "__support__/ui";

import Breadcrumbs from "./Breadcrumbs";

function setup(props = {}) {
  return renderWithProviders(
    <Route path="/" component={() => <Breadcrumbs {...props} />} />,
    { withRouter: true },
  );
}

describe("Breadcrumbs", () => {
  it("should render breadcrumb navigation with proper accessibility attributes", () => {
    const crumbs = [
      ["Home", "/"],
      ["Dashboard", "/dashboard"],
      ["Current Page"]
    ];

    setup({ crumbs });

    const nav = screen.getByRole("navigation", { name: "Breadcrumb navigation" });
    expect(nav).toBeInTheDocument();

    const list = screen.getByRole("list");
    expect(list).toBeInTheDocument();

    const listItems = screen.getAllByRole("listitem");
    expect(listItems).toHaveLength(5); // 3 breadcrumbs + 2 separators
  });

  it("should mark the last breadcrumb with aria-current=page", () => {
    const crumbs = [
      ["Home", "/"],
      ["Current Page"]
    ];

    setup({ crumbs });

    const currentPageLink = screen.getByText("Current Page");
    expect(currentPageLink).toHaveAttribute("aria-current", "page");
  });

  it("should support keyboard navigation for clickable breadcrumbs", async () => {
    const mockAction = jest.fn();
    const crumbs = [
      ["Home", "/"],
      ["Clickable Item", mockAction]
    ];

    setup({ crumbs });

    const clickableItem = screen.getByRole("button", { name: "Clickable Item" });
    expect(clickableItem).toBeInTheDocument();

    // Test Enter key
    await userEvent.type(clickableItem, "{enter}");
    expect(mockAction).toHaveBeenCalledTimes(1);

    // Test Space key
    await userEvent.type(clickableItem, " ");
    expect(mockAction).toHaveBeenCalledTimes(2);
  });

  it("should hide separators from screen readers", () => {
    const crumbs = [
      ["Home", "/"],
      ["Dashboard", "/dashboard"]
    ];

    setup({ crumbs });

    const separators = screen.getAllByRole("listitem").filter(item => 
      item.hasAttribute("aria-hidden") && item.getAttribute("aria-hidden") === "true"
    );
    expect(separators).toHaveLength(1);
  });

  it("should render placeholder when no crumbs are provided", () => {
    setup({ placeholder: "No navigation" });

    expect(screen.getByText("No navigation")).toBeInTheDocument();
  });
});