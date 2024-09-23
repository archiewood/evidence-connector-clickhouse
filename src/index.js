/**
 * This type describes the options that your connector expects to recieve
 * This could include username + password, host + port, etc
 * @typedef {Object} ConnectorOptions
 * @property {string} SomeOption
 */

import { EvidenceType } from "@evidence-dev/db-commons";
import { createClient } from '@clickhouse/client';

/**
 * @see https://docs.evidence.dev/plugins/creating-a-plugin/datasources#options-specification
 * @see https://github.com/evidence-dev/evidence/blob/main/packages/postgres/index.cjs#L316
 */
export const options = {
  url: {
    title: "URL",
    description: "Clickhouse instance URL",
    type: "string", 
  },
  username: {
    title: "Username",
    type: "string",
  },
  password: {
    title: "Password",
    type: "string",
    secret: true
  },

};

/**
 * Implementing this function creates a "file-based" connector
 *
 * Each file in the source directory will be passed to this function, and it will return
 * either an array, or an async generator {@see https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/async_function*}
 * that contains the query results
 *
 * @see https://docs.evidence.dev/plugins/creating-a-plugin/datasources#simple-interface-arrays
 * @type {import("@evidence-dev/db-commons").GetRunner<ConnectorOptions>}
 */
export const getRunner = (options) => {
  console.debug(`SomeOption = ${options.SomeOption}`);

  const client = createClient({
    url: options.url,
    username: options.username,
    password: options.password,
  });

  return async (queryText, queryPath) => {
    try {
      const rows = await client.query({
        query: queryText,
        format: 'JSONEachRow',
      });

      const result = await rows.json();

      // Transform the result into the expected output format
      const output = {
        rows: result,
        columnTypes: Object.keys(result[0] || {}).map((key) => ({
          name: key,
          evidenceType: typeof result[0][key] === 'number' ? EvidenceType.NUMBER : EvidenceType.STRING,
          typeFidelity: 'inferred',
        })),
        expectedRowCount: result.length,
      };

      return output;
    } catch (error) {
      console.error('Error executing query:', error);
      throw error;
    }
  };
};

// Uncomment to use the advanced source interface
// This uses the `yield` keyword, and returns the same type as getRunner, but with an added `name` and `content` field (content is used for caching)
// sourceFiles provides an easy way to read the source directory to check for / iterate through files
// /** @type {import("@evidence-dev/db-commons").ProcessSource<ConnectorOptions>} */
// export async function* processSource(options, sourceFiles, utilFuncs) {
//   yield {
//     title: "some_demo_table",
//     content: "SELECT * FROM some_demo_table", // This is ONLY used for caching
//     rows: [], // rows can be an array
//     columnTypes: [
//       {
//         name: "someInt",
//         evidenceType: EvidenceType.NUMBER,
//         typeFidelity: "inferred",
//       },
//     ],
//   };
//   yield {
//     title: "some_demo_table",
//     content: "SELECT * FROM some_demo_table", // This is ONLY used for caching
//     rows: async function* () {}, // rows can be a generator function for returning batches of results (e.g. if an API is paginated, or database supports cursors)
//     columnTypes: [
//       {
//         name: "someInt",
//         evidenceType: EvidenceType.NUMBER,
//         typeFidelity: "inferred",
//       },
//     ],
//   };

//  throw new Error("Process Source has not yet been implemented");
// }

/**
 * Implementing this function creates an "advanced" connector
 *
 *
 * @see https://docs.evidence.dev/plugins/creating-a-plugin/datasources#advanced-interface-generator-functions
 * @type {import("@evidence-dev/db-commons").GetRunner<ConnectorOptions>}
 */

/** @type {import("@evidence-dev/db-commons").ConnectionTester<ConnectorOptions>} */
export const testConnection = async (opts) => {
  return true;
};
