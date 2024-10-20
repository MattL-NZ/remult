### Create a SvelteKit Project

To create a new SvelteKit project, run the following command:

```sh
npm create svelte@latest remult-sveltekit-todo
```

During the setup, answer the prompts as follows:

1. **Which Svelte app template?**: `Skeleton` Project
2. **Add type checking with TypeScript?**: Yes, using `TypeScript` syntax
3. **Select additional options**: You can optionally include _Prettier_ and _ESLint_, but these are not required for this tutorial.

Once the setup is complete, navigate into the project directory:

```sh
cd remult-sveltekit-todo
```

### Install Required Packages and Remult

Install Remult and any necessary dependencies by running:

```sh
npm install remult --save-dev
```

### Bootstrap Remult

To set up Remult in your SvelteKit project:

1. **Create an API File**

   In the `src/server/` directory, create an `api.ts` file to handle Remult API requests:

   ```ts [src/server/api.ts]
   import { remultSveltekit } from 'remult/remult-sveltekit'

   export const api = remultSveltekit({})
   ```

2. **Create a SvelteKit Route**

   Now, create a route for Remult:

   ```ts [src/routes/api/[...remult]/+server.ts]
   import { api } from '../../../server/api'

   export const { GET, POST, PUT, DELETE } = api
   ```

### Final Tweaks

Remult uses TypeScript decorators to enhance classes into entities. To enable decorators in your SvelteKit project, modify the `tsconfig.json` file by adding the following to the `compilerOptions` section:

```json [tsconfig.json]
{
  "compilerOptions": {
    "experimentalDecorators": true // [!code ++]
  }
}
```

### Run the App

To start the development server, run the following command:

```sh
npm run dev
```

Your SvelteKit app will be available at [http://localhost:5173](http://localhost:5173).

### Setup Completed

Your SvelteKit project with Remult is now up and running. You can now proceed to the [next step of the tutorial](./entities.md) to start building your task list app.
