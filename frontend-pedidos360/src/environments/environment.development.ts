export const environment = {
  production: false,

  msal: {
    clientId: 'c4d519d1-24b9-4d49-b6fb-c3d446debcec',
    authority: 'https://pedidos360kurth.ciamlogin.com/6fc67620-759d-4763-ae40-3ebdfa5392eb/',
    knownAuthorities: ['pedidos360kurth.ciamlogin.com'],
    redirectUri: 'http://localhost:4200',
    postLogoutRedirectUri: 'http://localhost:4200',
  },

  api: {
    baseUrl: 'https://61m2ifom2j.execute-api.us-east-1.amazonaws.com',
    scopes: [
      'api://b9553765-6e89-4f61-b47f-90ff407b1bcc/Pedidos360.Read',
      'api://b9553765-6e89-4f61-b47f-90ff407b1bcc/Pedidos360.Write',
    ],
  },
};
