import { basicSetup } from "codemirror";
import { Compartment, EditorState } from "@codemirror/state";
import { EditorView, keymap } from "@codemirror/view";
import { autocompletion, closeBracketsKeymap, completionKeymap } from "@codemirror/autocomplete";
import { indentWithTab } from "@codemirror/commands";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { sql, SQLite } from "@codemirror/lang-sql";
import { tags } from "@lezer/highlight";

const schemaCompartment = new Compartment();

const sqlHighlightStyle = HighlightStyle.define([
  { tag: tags.keyword, color: "#c678dd", fontWeight: "700" },
  { tag: [tags.operatorKeyword, tags.controlKeyword], color: "#c678dd", fontWeight: "700" },
  { tag: [tags.function(tags.variableName), tags.function(tags.propertyName)], color: "#61afef" },
  { tag: [tags.string, tags.special(tags.string)], color: "#98c379" },
  { tag: [tags.number, tags.integer, tags.float], color: "#d19a66" },
  { tag: [tags.comment, tags.lineComment, tags.blockComment], color: "#7f8c98", fontStyle: "italic" },
  { tag: [tags.bool, tags.null], color: "#e06c75", fontWeight: "700" },
  { tag: [tags.definition(tags.name), tags.standard(tags.name)], color: "#e5edf7" },
]);

function buildSchemaTables(tables, columns) {
  const tableMap = {};
  for (const table of tables) {
    tableMap[table] = columns;
  }
  return tableMap;
}

function buildExtensions(schemaConfig) {
  return [
    keymap.of([indentWithTab, ...closeBracketsKeymap, ...completionKeymap]),
    EditorView.lineWrapping,
    syntaxHighlighting(sqlHighlightStyle),
    EditorView.theme({
      "&": {
        height: "240px",
        fontSize: "14px",
        fontFamily: "\"IBM Plex Mono\", monospace",
        backgroundColor: "#171c26",
        color: "#e6edf3",
      },
      "&.cm-focused": {
        outline: "none",
      },
      ".cm-scroller": {
        overflow: "auto",
        fontFamily: "\"IBM Plex Mono\", monospace",
      },
      ".cm-content": {
        padding: "18px",
        caretColor: "#f8fafc",
      },
      ".cm-activeLine": {
        backgroundColor: "#243042",
      },
      ".cm-selectionBackground, ::selection": {
        backgroundColor: "#2d4b6a !important",
      },
      ".cm-gutters": {
        display: "none",
      },
      ".cm-tooltip-autocomplete": {
        backgroundColor: "#111827",
        color: "#e5edf7",
        borderRadius: "14px",
        border: "1px solid #d9d2c0",
        overflow: "hidden",
        boxShadow: "0 18px 40px rgba(0, 0, 0, 0.28)",
      },
      ".cm-tooltip-autocomplete ul": {
        fontFamily: "\"IBM Plex Mono\", monospace",
        backgroundColor: "#111827",
      },
      ".cm-tooltip-autocomplete ul li": {
        color: "#dbe6f3",
        padding: "8px 12px",
      },
      ".cm-tooltip-autocomplete ul li[aria-selected]": {
        backgroundColor: "#1d4ed8",
        color: "#ffffff",
      },
      ".cm-tooltip-autocomplete ul li[aria-selected] .cm-completionMatchedText": {
        color: "#ffffff",
      },
      ".cm-completionMatchedText": {
        color: "#8ec5ff",
        fontWeight: "700",
        textDecoration: "none",
      },
    }),
    autocompletion({
      activateOnTyping: true,
      defaultKeymap: true,
      icons: false,
      closeOnBlur: true,
    }),
    schemaCompartment.of(sql(schemaConfig)),
  ];
}

export function createSQLEditor({ parent, initialDoc = "", onChange, schema }) {
  const schemaConfig = {
    dialect: SQLite,
    schema: buildSchemaTables(schema.tables || [], schema.columns || []),
    defaultTable: schema.tables?.includes("logs") ? "logs" : schema.tables?.[0],
    upperCaseKeywords: true,
  };

  const state = EditorState.create({
    doc: initialDoc,
    extensions: [
      ...buildExtensions(schemaConfig),
      EditorView.updateListener.of((update) => {
        if (update.docChanged && typeof onChange === "function") {
          onChange(update.state.doc.toString());
        }
      }),
    ],
  });

  const view = new EditorView({
    state,
    parent,
  });

  return {
    view,
    setSchema(nextSchema) {
      const nextConfig = {
        dialect: SQLite,
        schema: buildSchemaTables(nextSchema.tables || [], nextSchema.columns || []),
        defaultTable: nextSchema.tables?.includes("logs") ? "logs" : nextSchema.tables?.[0],
        upperCaseKeywords: true,
      };
      view.dispatch({
        effects: schemaCompartment.reconfigure(sql(nextConfig)),
      });
    },
    setValue(value) {
      const current = view.state.doc.toString();
      if (current === value) return;
      view.dispatch({
        changes: {
          from: 0,
          to: current.length,
          insert: value,
        },
      });
    },
    getValue() {
      return view.state.doc.toString();
    },
    focus() {
      view.focus();
    },
    destroy() {
      view.destroy();
    },
  };
}

window.Log2SQL.createSQLEditor = createSQLEditor;
