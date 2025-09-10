---
title: Text and markdown on dashboards
summary: Add text cards to your dashboard and use markdown format text and add images,
---

Metabase has two types of cards for displaying text: heading cards and text cards. Text cards allow you to include descriptions, explanations, notes, or even images and GIFs to your dashboards. You can also use text and heading cards to create separations between sections of charts in your dashboards, or include links to other dashboards, questions, or websites.

---

## Add a text or heading card

To add a new text card, create a new dashboard (or edit an existing one) and click on the text card button, **T**, in the top-right:

![Text card button](images/text-card-button.png)

You have two options:

- **Heading**: a preformatted heading text card that spans the width of the dashboard.
- **Text**: a customizable text card that will render Markdown-formatted text.

Each text card has two modes: writing and previewing. When you click to focus on the card, the card will enter editing mode. When you click away from the card, Metabase will render the card to show you what it will look like on the live dashboard.

![Markdown](images/markdown.png)

You can use [Markdown](http://commonmark.org/help/) to format the text in your text card, create inline tables or code snippets, or even embed linked images (easy on the GIFs, friends). To preview the rendered card, just click away from the card.

![Result](images/result.png)

To learn more, see [Fun with Markdown in your dashboards](https://www.metabase.com/learn/metabase-basics/querying-and-dashboards/dashboards/markdown).

---

Select a dashboard you'd like to add text to, and click on the **pencil icon** to edit the dashboard. Click on the **text box** button in the dashboard toolbar (shown as `Aa`). A text box will appear, which you can move around just as you would a question box. To resize the text box, click and drag on its bottom right corner.

## Writing in the text box

To interact with a text box, you'll need to be in edit mode in a dashboard. Click on the **pencil icon** in the dashboard toolbar in the upper right of the dashboard.

Text boxes in Metabase have two modes.

- Edit text (**pencil icon**).
- Rendered view (**eye icon**).

## Configure text cards

## How Markdown works

The text editor in Metabase employs a lightweight markup language called Markdown. If you've never used Markdown before, it takes some getting used to, but you'll soon learn to appreciate its simplicity. Markdown can make writers feel like coders and coders feel at home.

You can use Markdown syntax to add links, images, gifs, lists, codeblocks, blockquotes, and more. Here's a text box with heading, paragraph, blockquote, and code block:

![Heading, paragraph, and blockquote.](../../../images/fun-with-markdown-in-your-dashboards/heading-paragraph-blockquote.png)

You can do everything this post does and more using text boxes in Metabase. The big deal with Markdown is that you don't have to write tedious HTML, and Markdown is human-readable even before Metabase renders it. Markdown's minimalist feature set will keep you focused on the content, and provide a standardized look across your dashboards.

You can learn more about Markdown syntax [in this guide](https://www.markdownguide.org/), as well as from [one of Markdown's original creators](https://daringfireball.net/projects/markdown/syntax), which also includes the [philosophy](https://daringfireball.net/projects/markdown/syntax#philosophy) behind Markdown. As a bonus, the site allows you to view its content in [Markdown syntax](https://daringfireball.net/projects/markdown/syntax.text).

## Example text box

Here is an example dashboard with a question and text box:

![Dashboard with question and text cards.](../../../images/fun-with-markdown-in-your-dashboards/dashboard-with-text-box.png)

Here is the Markdown code used in the text box above:

```md
# Analysis

Although `Gadgets` outsold `Gizmos` in 2019, we only introduced `Gizmos` and `Doohickeys` in September of 2019. Additionally, both `Gadgets` and `Widgets` were heavily discounted during our spring, summer, and holiday sales.

We expect sales to continue to grow in the `Gizmo` and `Doohickey` product lines.

# SQL query

    SELECT "PRODUCTS__via__PRODUCT_ID"."CATEGORY" AS "CATEGORY",
    sum("PUBLIC"."ORDERS"."QUANTITY") AS "sum"
    FROM "PUBLIC"."ORDERS"
    LEFT JOIN "PUBLIC"."PRODUCTS" "PRODUCTS__via__PRODUCT_ID" ON
    "PUBLIC"."ORDERS"."PRODUCT_ID" = "PRODUCTS__via__PRODUCT_ID"."ID"
    WHERE ("PUBLIC"."ORDERS"."CREATED_AT" >= timestamp with time zone '2019-01-01 00:00:00.000Z'
    AND "PUBLIC"."ORDERS"."CREATED_AT" < timestamp with time zone '2020-01-01 00:00:00.000Z')
    GROUP BY "PRODUCTS__via__PRODUCT_ID"."CATEGORY"
    ORDER BY "sum" ASC, "PRODUCTS__via__PRODUCT_ID"."CATEGORY" ASC

# Contact

If you have questions, reach out to us on the [\#product](https://fakemessageservice.com/product) channel.
```

The hypothetical analyst provided some context, code, and contact info: but you can include whatever context will help readers of your report.

Note: in the example above, the analyst pasted the raw SQL query generated by the query builder for reference. You can view the SQL composed by the query builder by clicking on the **editor icon** to ["View the SQL"](/docs/latest/questions/query-builder/editor#viewing-the-native-query-that-powers-your-question) when in editing mode.

You can also use text boxes as simple dividers to keep your dashboards organized.

![Using a text card as a divider to organize your dashboard.](../../../images/fun-with-markdown-in-your-dashboards/text-box-divider.png)

## Heading cards and markdown headers

To create different heading levels like this:

![Headings as Metabase renders them in a text card on a dashboard.](../../../images/fun-with-markdown-in-your-dashboards/headings.png)

You'd write:

```md
# Heading 1

## Heading 2

### Heading 3

#### Heading 4

##### Heading 5

###### Heading 6
```

The plain text `## Heading 2` is rendered as the HTML code:

```html
<h2>
  Heading
  <h2></h2>
</h2>
```

## Make text bold or italic

## Create a list

## Add a code snippet

## Add a link

## Add a static table

## Add an image or GIF

## Add variables to text cards and connect to dashboard filters

You can include a variable in a text card, then wire that variable up to a dashboard filter. All you need to do to create a variable is to wrap a word in double braces, `{% raw %}{{{% endraw %}` and `{% raw %}}}{%endraw%}` (the variable can't contain any spaces). For example, you could add a text card with the following text:

```
{% raw %}
# {{state}} orders
{% endraw %}
```

And connect that variable to a dashboard filter widget that filters for states. If someone selected `WI` in the state filter, the text in the markdown card would read: **WI orders**.

You can also make text optional by wrapping the text in double brackets, `[[` and `]]`:

```
{% raw %}
# Orders [[from {{state}}]
{% endraw %}
```

In this case, the phrase `{% raw %}from {{state}}{%endraw%}` would only display if someone selected a value (or values) in the filter widget.

To see how to wire up a filter to a card, see [dashboard filters](./filters.md).

Besides text, You can use text cards to your dashboards to display text and images, add links, and format your text as bold, italic, list, etc. Metabase supports [Github-flavored markdown

You can also use variables in header and text cards to wire up your text cards to dashboard filters.

---

## Use variables in text cards to create dynamic text

You can [add variables](/docs/latest/dashboards/introduction#including-variables-in-text-cards) to text cards and wire them up to filters. Metabase will take values selected in the filter and plug those values into variables in your text, creating text cards that update automatically when people change the filter values.

For example, let's say you want to have a text card display the values in the **Plan** filter on your dashboard, like so:

![Metabase rendering the two plans selected in the filter,](../../../images/fun-with-markdown-in-your-dashboards/rendered-parameters.png)

The plans listed in the text card will change depending on which plans are selected in the filter (in this case, the Business and Premium plans were selected in the filter, and so the text card displays them).

To wire up a text card variable to a filter:

1. Click the **pencil** icon to enter dashboard edit mode.
2. [Add a filter](/docs/latest/dashboards/filters#adding-a-filter-or-parameter) to your dashboard.
3. [Add a text card](/docs/latest/dashboards/introduction) to your dashboard.
4. Write some Markdown and include a variable. Variables are bookended with double braces:

   ```liquid
   {%raw%}# Plan
   ## {{PLAN}}{%endraw%}
   ```

5. [Connect the filter](/docs/latest/dashboards/filters#editing-a-filter) to the variable in the text card.

![Adding a variable to your text card.](../../../images/fun-with-markdown-in-your-dashboards/parameter.png)

If no value is plugged into the filter, Metabase will render the unsightly `{%raw%}{{PLAN}}{%endraw%}` variable. To handle cases where the filter has no value, you can set a default value or, better, hide the text by surrounding the variable text with double brackets to make the text optional.

```liquid
{%raw%}# Plan
[[## {{PLAN}}]]{%endraw%}
```

The double brackets tell Metabase to display the text only if the connected filter has at least one value.

## Create a custom URL with a filter value

You can add a URL to a Markdown card like this:

```md
[Google Search](https://google.com)
```

which will show up as the clickable hyperlink: [Google Search](https://google.com).

To make a dynamic URL, such as a [Google Search for "filter value"](https://google.com/search?q=filter_value), you can put a `{%raw%}{{variable}}{%endraw%}` wherever you want the filter value text to show up. For example, to add a dynamic URL to a dashboard with the **Invoices** table:

1. Create a dashboard filter for "Plan".
2. Add a Markdown card with your URL and variables:

   ```liquid
   [[ [Google Search for "{%raw%}{{plan}}{%endraw%}"](https://google.com/search?q={%raw%}{{plan}}{%endraw%}) ]]
   ```

3. [Connect the "Plan" filter to the Markdown card](/docs/latest/dashboards/filters#editing-a-filter).
4. Optional: set a default value for the "Plan" filter.

The outer double square brackets in the Markdown card text will hide the URL by default when the filter is empty (no value selected and no default value set).

![A custom URL that accepts a filter value.](../../../images/fun-with-markdown-in-your-dashboards/custom-url.png)

Now, if someone goes to the "Plan" filter and selects "Basic", they'll see a clickable link in the Markdown card like this: [Google Search for "Basic"](https://google.com?q=basic).

## Custom URL with a sandboxing attribute

{% include plans-blockquote.html feature="Data sandboxing" %}

Say you have a "department" attribute and you want to create a custom link to a user guide like this:

```md
[View Marketing guide](https://your-company-wiki.com/marketing)
```

To display custom URLs based on a person's [sandboxing attribute](/learn/metabase-basics/administration/permissions/data-sandboxing-row-permissions#creating-an-account-and-adding-an-attribute):

1. Create a dashboard filter for the sandboxing attribute.
2. Add a Markdown card with your URL and variables:

   ```liquid
   [[ [View {%raw%}{{department}}{%endraw%} guide](https://your-company-wiki.com/{%raw%}{{department}}{%endraw%}) ]]
   ```

3. [Connect the filter to the Markdown card](/docs/latest/dashboards/filters#editing-a-filter).

To hide the URL by default when the filter value is empty, make sure to include the outer double square brackets in the Markdown card text.

When a sandboxed user views the dashboard, they'll see:

- [View Marketing guide](/blog/why-a-growth-marketing-team-wants-metabase).
- A filter widget with the value "Marketing" (and no other selectable options).
- Dashboard data that's [sandboxed on rows](/learn/metabase-basics/administration/permissions/data-sandboxing-row-permissions) where "Department = Marketing".

If your dashboard is a static embed, you can opt to [hide the filter widget](/docs/latest/embedding/static-embedding-parameters#hiding-filter-widgets-from-a-static-embed).

## One last pro tip for GIF aficionados

The image syntax,

```md
![image description](image-link)
```

also works for GIFs. Because there are far more important use cases for dashboard text boxes:

![One of these cards is not like the others.](../../../images/fun-with-markdown-in-your-dashboards/shades_off_dashboard.gif)

Happy Markdowning!
