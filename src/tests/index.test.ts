import Elastic from "../Elastic";

describe("connection", () => {
  test("able to connect", async () => {
    let isConnect = true;
    try {
      await Elastic.establish("http://localhost", "9200");
    } catch (e: unknown) {
      isConnect = false;
    }
    expect(isConnect).toBe(true);
  });

  test("throw error given elastic is not up", async () => {
    const fakePort = "9999";

    let isConnect = true;
    try {
      await Elastic.establish("http://localhost", fakePort);
    } catch (e: unknown) {
      isConnect = false;
      expect((e as Error).message).toMatch(/^Cannot establish elasticsearch connection:/);
    }
    expect(isConnect).toBe(false);
  });
});

describe("operations", () => {
  let elastic: Elastic;
  const index = "unit-test-elastic-index";
  const type = "typeName";

  beforeAll(async () => {
    try {
      elastic = await Elastic.establish("http://localhost", "9200");
    } catch (e) {
      throw new Error("Cannot connect");
    }
  });

  afterAll(async () => {
    await elastic.close();
  });

  afterEach(async () => {
    try {
      await elastic.deleteIndex(index);
    } catch (e) {
      throw new Error("Failed to delete elastic index: " + index);
    }
  });

  test("able to search", async () => {
    await elastic.createDocument(index, type, { keyName: "aa" });
    await elastic.createDocument(index, type, { keyName: "aa bb" });
    await elastic.createDocument(index, type, { keyName: "cc" });
    const result = await elastic.searchByField(index, type, "keyName", "aa");
    expect(result.length).toBe(2);
    expect(result[0].keyName).toBe("aa");
    expect(result[1].keyName).toBe("aa bb");
  });

  test("able to get all", async () => {
    await elastic.createDocument(index, type, { keyName: "value1" });
    await elastic.createDocument(index, type, { keyName: "value2" });
    const result = await elastic.getAll(index, type);

    expect(result.length).toBe(2);
    expect(result[0].keyName).toBe("value1");
    expect(result[1].keyName).toBe("value2");
  });

  test("able to create, get, update and delete", async () => {
    const document = { keyName: "value" };

    const id = await elastic.createDocument(index, type, document);
    expect(id.length).toBeGreaterThan(0);

    const result = await elastic.getById(index, type, id);
    expect(result).toEqual(document);

    const editedDoc = { keyName: "editedValue" };
    await elastic.updateDocument(index, type, id, editedDoc);
    const doc = await elastic.getById(index, type, id);
    expect(doc).toEqual(editedDoc);

    expect(async () => await elastic.deleteDocument(index, type, id)).not.toThrow();
    expect(await elastic.getById(index, type, id)).toBe(undefined);
  });
});
