import cx from "classnames";
import { Fragment } from "react";
import { Link } from "react-router";

import { Ellipsified } from "metabase/common/components/Ellipsified";
import { Icon } from "metabase/ui";

import S from "./Breadcrumbs.module.css";

type BreadcrumbItem = string | [string] | [string, string | (() => void)];

interface BreadcrumbsProps {
  /** Additional CSS class name */
  className?: string;
  /** Array of breadcrumb items. Each item can be a string or tuple of [title, url/action] */
  crumbs?: BreadcrumbItem[];
  /** Whether the breadcrumbs are displayed in a sidebar */
  inSidebar?: boolean;
  /** Placeholder text to show when there are no crumbs */
  placeholder?: string;
  /** Size variant for the breadcrumbs */
  size?: "medium" | "large";
}

/**
 * Breadcrumb navigation component that displays hierarchical navigation paths.
 * Supports both static breadcrumbs and interactive ones with click handlers.
 * 
 * TODO: merge with BrowserCrumbs component
 */
export default function Breadcrumbs({
  className,
  crumbs = [],
  inSidebar = false,
  placeholder = null,
  size = "medium",
}: BreadcrumbsProps) {
  const breadcrumbClass = inSidebar ? S.sidebarBreadcrumb : S.breadcrumb;
  const breadcrumbsClass = inSidebar ? S.sidebarBreadcrumbs : S.breadcrumbs;

  return (
    <nav
      aria-label="Breadcrumb navigation"
      data-testid="breadcrumbs"
      className={cx(className, breadcrumbsClass)}
    >
      <ol className={S.breadcrumbList}>
        {crumbs.length <= 1 && placeholder ? (
          <li>
            <span className={cx(breadcrumbClass, S.breadcrumbPage)}>
              {placeholder}
            </span>
          </li>
        ) : (
          crumbs
            .map((breadcrumb) =>
              Array.isArray(breadcrumb) ? breadcrumb : [breadcrumb],
            )
            .map((breadcrumb, index, allCrumbs) => {
              const isLast = index === allCrumbs.length - 1;
              const hasLink = breadcrumb.length > 1;

              return (
                <Fragment key={index}>
                  <li className={cx(
                    breadcrumbClass,
                    hasLink ? S.breadcrumbPath : S.breadcrumbPage,
                    { [S.fontLarge]: size === "large" },
                  )}>
                    <Ellipsified
                      tooltip={breadcrumb[0]}
                      tooltipMaxWidth="auto"
                    >
                      {hasLink && typeof breadcrumb[1] === "string" ? (
                        <Link 
                          to={breadcrumb[1]}
                          aria-current={isLast ? "page" : undefined}
                        >
                          {breadcrumb[0]}
                        </Link>
                      ) : (
                        <span 
                          onClick={breadcrumb[1]}
                          role={breadcrumb[1] ? "button" : undefined}
                          tabIndex={breadcrumb[1] ? 0 : undefined}
                          onKeyDown={breadcrumb[1] ? (e) => {
                            if (e.key === "Enter" || e.key === " ") {
                              e.preventDefault();
                              if (typeof breadcrumb[1] === "function") {
                                breadcrumb[1]();
                              }
                            }
                          } : undefined}
                          aria-current={isLast ? "page" : undefined}
                        >
                          {breadcrumb[0]}
                        </span>
                      )}
                    </Ellipsified>
                  </li>
                  {!isLast && (
                    <li aria-hidden="true">
                      <Icon
                        name="chevronright"
                        className={S.breadcrumbDivider}
                        width={12}
                        height={12}
                      />
                    </li>
                  )}
                </Fragment>
              );
            })
        )}
      </ol>
    </nav>
  );
}
