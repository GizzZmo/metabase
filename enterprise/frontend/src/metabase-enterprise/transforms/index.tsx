import { IndexRoute } from "react-router";
import { t } from "ttag";

import { createAdminRouteGuard } from "metabase/admin/utils";
import { Route } from "metabase/hoc/Title";
import { PLUGIN_TRANSFORMS } from "metabase/plugins";

import { NewTransformQueryPage } from "./pages/NewTransformQueryPage";
import { RunsPage } from "./pages/RunsPage";
import { TransformListPage } from "./pages/TransformListPage";
import { TransformPage } from "./pages/TransformPage";
import { TransformPageLayout } from "./pages/TransformPageLayout";
import { TransformQueryPage } from "./pages/TransformQueryPage";

PLUGIN_TRANSFORMS.getAdminPaths = () => [
  { key: "transforms", name: t`Transforms`, path: "/admin/transforms" },
];

PLUGIN_TRANSFORMS.getAdminRoutes = () => (
  <Route path="transforms" component={createAdminRouteGuard("transforms")}>
    <Route title={t`Transforms`}>
      <Route component={TransformPageLayout}>
        <IndexRoute component={TransformListPage} />
        <Route path="runs" component={RunsPage} />
        <Route path=":transformId" component={TransformPage} />
      </Route>
      <Route path="new/:type" component={NewTransformQueryPage} />
      <Route path="new/card/:cardId" component={NewTransformQueryPage} />
      <Route path=":transformId/query" component={TransformQueryPage} />
    </Route>
  </Route>
);
