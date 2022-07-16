module.exports = {
  env: {
    browser: true,
    es2021: true,
    node: true,
  },
  extends: [
    "eslint:recommended",
    "plugin:@typescript-eslint/recommended",
    "plugin:import/recommended",
    "plugin:import/typescript",
    "prettier",
    "plugin:prettier/recommended",
  ],
  parser: "@typescript-eslint/parser",
  parserOptions: {
    ecmaVersion: "latest",
    sourceType: "module",
  },
  plugins: ["@typescript-eslint", "prettier"],
  rules: {
    // turn on errors for missing imports
    "import/no-unresolved": "error",
    "prettier/prettier": "error",
  },
  settings: {
    "import/parsers": {
      "@typescript-eslint/parser": [".ts"],
    },
    "import/resolver": {
      typescript: {
        // always try to resolve types under `<root>@types` directory even it doesn't contain any source code, like `@types/unist`
        alwaysTryTypes: true,

        // use <root>/path/to/folder/tsconfig.json
        project: "./tsconfig.json",
        // Multiple tsconfigs (Useful for monorepos)
        // use an array
        // "project": [
        //   "packages/module-a/tsconfig.json",
        //   "packages/module-b/tsconfig.json"
        // ],
      },
    },
  },
};
