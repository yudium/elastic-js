import { Client } from "@elastic/elasticsearch";

type Body = { [key: string]: string | string[] };

type DocumentId = string;

/**
 * DECISION: I use adapter-like pattern in this class for preventing any
 * version major break on elasticsearch package in future
 *
 * @see https://www.elastic.co/guide/en/cloud/current/ec-getting-started-node-js.html
 */
export default class Elastic {
  private constructor(private client: Client) {}

  static async establish(host?: string, port?: string) {
    if (typeof host !== "string") {
      throw new Error("Host invalid");
    }
    if (typeof port !== "string") {
      throw new Error("Host invalid");
    }

    const client = new Client({
      node: host + ":" + port,
    });

    try {
      await client.info();
      return new Elastic(client);
    } catch (e) {
      throw new Error("Cannot establish elasticsearch connection: " + e);
    }
  }

  /**
   * Make sure new index follows convention
   * because elasticsearch doesnt allow camel case.
   * @param name string
   * @returns undefined
   */
  validateIndex(name: string) {
    const isNotValid = null === name.match(/^[a-z0-9\-]+$/);
    if (isNotValid) {
      throw new Error("Only snake-case allowed");
    }
  }

  async createDocument(index: string, type: string, body: Body): Promise<DocumentId> {
    this.validateIndex(index);

    const result = await this.client.index({
      index,
      type,
      body,
      refresh: "true",
    });
    await this.refresh(index);
    if (result.body.result !== "created" || result.statusCode !== 201) {
      throw new Error("Failed to create document");
    }
    return result.body._id;
  }

  async getById(index: string, type: string, id: string): Promise<{ [key: string]: string | string[] } | undefined> {
    try {
      const { body } = await this.client.get({
        index,
        type,
        id,
      });
      return body._source;
    } catch (e: any) {
      if (e.body.found === false) {
        // it is not error but the data is not found
        return undefined;
      }
      throw e;
    }
  }

  async updateDocument(index: string, type: string, id: string, body: Body): Promise<boolean> {
    try {
      await this.client.update({
        index,
        type,
        id,
        refresh: "true",
        body: {
          doc: body,
        },
      });
      return true;
    } catch (e: any) {
      return false;
    }
  }

  async getAll(index: string, type: string): Promise<Body[]> {
    const result = await this.client.search({
      index,
      type,
      body: {
        sort: [{ _uid: { order: "asc" } }], // this one is used for predicted order in unit test
        query: {
          match_all: {},
        },
      },
    });
    return result.body.hits.hits.map((h: Body) => h._source);
  }

  async searchByField(index: string, type: string, field: string, query: string): Promise<Body[]> {
    const result = await this.client.search({
      index,
      type,
      body: {
        sort: [{ _uid: { order: "asc" } }], // this one is used for predicted order in unit test
        query: {
          match: { [field]: query },

          // other than above we can also use several search techniques that
          // explained by https://app.pluralsight.com/course-player?clipId=704351d5-be5e-4559-ad58-5e1b7763d40e.
          //
          // Let see:
          //
          // 0. Exact with term
          // not analyzed and should exact term:
          //
          //    term: { name: "Iphone" },
          //
          // will not matched "iphone"
          // @see https://stackoverflow.com/a/26003404
          //
          // 1. Basic
          // below code will match a document with name 'Iphone 12'
          //
          //    match: { name: "Iphone" },
          //
          // but if we search with 'Ipho' then it doesnt match.
          //
          // 2. Multi-match
          // search in multiple fields.
          //
          //    multi_match: {
          //      "query": "yourkeyword",
          //      fields: ["firstName", "address"],
          //    }
          //
          // 3. Match phrase
          // also we can use match_phrase to get documents that matched it and not broken up:
          //
          //    "match_phrase": {
          //      "address": "segwick street",
          //    }
          //
          // 4. Using Wildcard
          // we can use asterisk to match our pattern.
          //
          //    "wildcard": {
          //      "firstname": { "value": "h*ll" },
          //    }
          //
          // 4. Query string
          // Other option is using query with some operator:
          //
          //    "query_string": {
          //      "query": "(new york city) OR (big apple)",
          //      "default_field": "content"
          //    }
          //
          // @see https://stackoverflow.com/a/26003404
          // @see https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-dsl-query-string-query
          //
          // 5. Using regex:
          //
          //    regexp: { [field]: `.*${query}.*` },
          //
          // @see: https://stackoverflow.com/a/37711845
          //
          //
          // MANIPULATE SCORE ON RESULT
          //
          // 1. Negative term:
          // we can use negative term to reduce score as well:
          // @see https://app.pluralsight.com/course-player?clipId=4624e890-c7b9-4b08-9195-a6d34ee7e137
          //
          // 2. Constant score:
          // we also can add constant score if match
          // @see https://app.pluralsight.com/course-player?clipId=4624e890-c7b9-4b08-9195-a6d34ee7e137
          //
          // 3. Disjunction max:
          // i dont understand well but look at the resource:
          // @see https://app.pluralsight.com/course-player?clipId=4624e890-c7b9-4b08-9195-a6d34ee7e137
          //
          // 3. Function score:
          // this is complex and not explained enough by:
          // @see https://app.pluralsight.com/course-player?clipId=4624e890-c7b9-4b08-9195-a6d34ee7e137
        },
      },
    });

    return result.body.hits.hits.map((h: Body) => h._source);
  }

  async deleteDocument(index: string, type: string, id: DocumentId) {
    await this.client.delete({ index, type, id, refresh: "true" });
  }

  async deleteIndex(index: string): Promise<boolean> {
    await this.client.indices.delete({ index }, { ignore: [404] });
    return true;
  }

  /**
   * Should refresh after you finish with all non-query operations so that it
   * is ready to be searched. Like commit in SQL.
   */
  async refresh(index: string) {
    await this.client.indices.refresh({ index });
  }

  async close() {
    await this.client.close();
  }
}
