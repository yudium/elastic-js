import { Client } from '@elastic/elasticsearch';

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
    if (typeof host !== 'string') {
      throw new Error('Host invalid');
    }
    if (typeof port !== 'string') {
      throw new Error('Host invalid');
    }

    const client = new Client({
      node: host + ':' + port,
    });

    try {
      await client.info();
      return new Elastic(client);
    } catch (e) {
      throw new Error('Cannot establish elasticsearch connection: ' + e);
    }
  }

  async createDocument(index: string, type: string, body: Body): Promise<DocumentId> {
    const result = await this.client.index({
      index,
      type,
      body,
      refresh: 'true',
    });
    await this.refresh(index);
    if (result.body.result !== 'created' || result.statusCode !== 201) {
      throw new Error('Failed to create document');
    }
    return result.body._id;
  }

  async getById(
    index: string,
    type: string,
    id: string,
  ): Promise<{ [key: string]: string | string[] } | undefined> {
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
        refresh: 'true',
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
        query: {
          match_all: {},
        },
        sort: [{ _uid: { order: 'asc' } }],
      },
    });
    return result.body.hits.hits.map((h: Body) => h._source);
  }

  async searchByField(index: string, type: string, field: string, query: string): Promise<Body[]> {
    const result = await this.client.search({
      index,
      type,
      body: {
        query: {
          // below code will match 'Iphone 12' on 'Iphone' but not 'Iphon' query
          //    match: { [field]: query },
          // to support partial text then use regex:
          // @see: https://stackoverflow.com/a/37711845
          regexp: { [field]: `.*${query}.*` },
        },
        sort: [{ _uid: { order: 'asc' } }],
      },
    });

    return result.body.hits.hits.map((h: Body) => h._source);
  }

  async deleteDocument(index: string, type: string, id: DocumentId) {
    await this.client.delete({ index, type, id });
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
