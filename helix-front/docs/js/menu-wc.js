'use strict';

customElements.define('compodoc-menu', class extends HTMLElement {
    constructor() {
        super();
        this.isNormalMode = this.getAttribute('mode') === 'normal';
    }

    connectedCallback() {
        this.render(this.isNormalMode);
    }

    render(isNormalMode) {
        let tp = lithtml.html(`
        <nav>
            <ul class="list">
                <li class="title">
                    <a href="index.html" data-type="index-link">helix-front documentation</a>
                </li>

                <li class="divider"></li>
                ${ isNormalMode ? `<div id="book-search-input" role="search"><input type="text" placeholder="Type to search"></div>` : '' }
                <li class="chapter">
                    <a data-type="chapter-link" href="index.html"><span class="icon ion-ios-home"></span>Getting started</a>
                    <ul class="links">
                        <li class="link">
                            <a href="overview.html" data-type="chapter-link">
                                <span class="icon ion-ios-keypad"></span>Overview
                            </a>
                        </li>
                        <li class="link">
                            <a href="index.html" data-type="chapter-link">
                                <span class="icon ion-ios-paper"></span>README
                            </a>
                        </li>
                                <li class="link">
                                    <a href="dependencies.html" data-type="chapter-link">
                                        <span class="icon ion-ios-list"></span>Dependencies
                                    </a>
                                </li>
                                <li class="link">
                                    <a href="properties.html" data-type="chapter-link">
                                        <span class="icon ion-ios-apps"></span>Properties
                                    </a>
                                </li>
                    </ul>
                </li>
                    <li class="chapter modules">
                        <a data-type="chapter-link" href="modules.html">
                            <div class="menu-toggler linked" data-toggle="collapse" ${ isNormalMode ?
                                'data-target="#modules-links"' : 'data-target="#xs-modules-links"' }>
                                <span class="icon ion-ios-archive"></span>
                                <span class="link-name">Modules</span>
                                <span class="icon ion-ios-arrow-down"></span>
                            </div>
                        </a>
                        <ul class="links collapse " ${ isNormalMode ? 'id="modules-links"' : 'id="xs-modules-links"' }>
                            <li class="link">
                                <a href="modules/AppModule.html" data-type="entity-link" >AppModule</a>
                                    <li class="chapter inner">
                                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                            'data-target="#components-links-module-AppModule-00115ad779e30a3f298b926bdfee0b0231dab37993f3d2fa69bdbbcd282df9abdc1459289cb6e34e22e0b1bc0d7a69a0bf8b2adfc07c8120b4640cbb5276f2c5"' : 'data-target="#xs-components-links-module-AppModule-00115ad779e30a3f298b926bdfee0b0231dab37993f3d2fa69bdbbcd282df9abdc1459289cb6e34e22e0b1bc0d7a69a0bf8b2adfc07c8120b4640cbb5276f2c5"' }>
                                            <span class="icon ion-md-cog"></span>
                                            <span>Components</span>
                                            <span class="icon ion-ios-arrow-down"></span>
                                        </div>
                                        <ul class="links collapse" ${ isNormalMode ? 'id="components-links-module-AppModule-00115ad779e30a3f298b926bdfee0b0231dab37993f3d2fa69bdbbcd282df9abdc1459289cb6e34e22e0b1bc0d7a69a0bf8b2adfc07c8120b4640cbb5276f2c5"' :
                                            'id="xs-components-links-module-AppModule-00115ad779e30a3f298b926bdfee0b0231dab37993f3d2fa69bdbbcd282df9abdc1459289cb6e34e22e0b1bc0d7a69a0bf8b2adfc07c8120b4640cbb5276f2c5"' }>
                                            <li class="link">
                                                <a href="components/AppComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >AppComponent</a>
                                            </li>
                                        </ul>
                                    </li>
                            </li>
                            <li class="link">
                                <a href="modules/ChooserModule.html" data-type="entity-link" >ChooserModule</a>
                                    <li class="chapter inner">
                                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                            'data-target="#components-links-module-ChooserModule-53d69d0f373712df08a9a12269682ed69ab67496c123c045e5397a3ef215a0b20c48f79928792550bb31a1371e253cfc03ed496ca03a5f8fde395481dedc78f5"' : 'data-target="#xs-components-links-module-ChooserModule-53d69d0f373712df08a9a12269682ed69ab67496c123c045e5397a3ef215a0b20c48f79928792550bb31a1371e253cfc03ed496ca03a5f8fde395481dedc78f5"' }>
                                            <span class="icon ion-md-cog"></span>
                                            <span>Components</span>
                                            <span class="icon ion-ios-arrow-down"></span>
                                        </div>
                                        <ul class="links collapse" ${ isNormalMode ? 'id="components-links-module-ChooserModule-53d69d0f373712df08a9a12269682ed69ab67496c123c045e5397a3ef215a0b20c48f79928792550bb31a1371e253cfc03ed496ca03a5f8fde395481dedc78f5"' :
                                            'id="xs-components-links-module-ChooserModule-53d69d0f373712df08a9a12269682ed69ab67496c123c045e5397a3ef215a0b20c48f79928792550bb31a1371e253cfc03ed496ca03a5f8fde395481dedc78f5"' }>
                                            <li class="link">
                                                <a href="components/HelixListComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >HelixListComponent</a>
                                            </li>
                                        </ul>
                                    </li>
                                <li class="chapter inner">
                                    <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                        'data-target="#injectables-links-module-ChooserModule-53d69d0f373712df08a9a12269682ed69ab67496c123c045e5397a3ef215a0b20c48f79928792550bb31a1371e253cfc03ed496ca03a5f8fde395481dedc78f5"' : 'data-target="#xs-injectables-links-module-ChooserModule-53d69d0f373712df08a9a12269682ed69ab67496c123c045e5397a3ef215a0b20c48f79928792550bb31a1371e253cfc03ed496ca03a5f8fde395481dedc78f5"' }>
                                        <span class="icon ion-md-arrow-round-down"></span>
                                        <span>Injectables</span>
                                        <span class="icon ion-ios-arrow-down"></span>
                                    </div>
                                    <ul class="links collapse" ${ isNormalMode ? 'id="injectables-links-module-ChooserModule-53d69d0f373712df08a9a12269682ed69ab67496c123c045e5397a3ef215a0b20c48f79928792550bb31a1371e253cfc03ed496ca03a5f8fde395481dedc78f5"' :
                                        'id="xs-injectables-links-module-ChooserModule-53d69d0f373712df08a9a12269682ed69ab67496c123c045e5397a3ef215a0b20c48f79928792550bb31a1371e253cfc03ed496ca03a5f8fde395481dedc78f5"' }>
                                        <li class="link">
                                            <a href="injectables/ChooserService.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >ChooserService</a>
                                        </li>
                                    </ul>
                                </li>
                            </li>
                            <li class="link">
                                <a href="modules/ClusterModule.html" data-type="entity-link" >ClusterModule</a>
                                    <li class="chapter inner">
                                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                            'data-target="#components-links-module-ClusterModule-20fc81e0e913fd0da9a57fdc2994ac39bda50fc1dde5c4c4b73f51b6f87724897be405741f77c3ef21e8ee10e0de19d864ea2fcfed2816e0aca7576eb310d489"' : 'data-target="#xs-components-links-module-ClusterModule-20fc81e0e913fd0da9a57fdc2994ac39bda50fc1dde5c4c4b73f51b6f87724897be405741f77c3ef21e8ee10e0de19d864ea2fcfed2816e0aca7576eb310d489"' }>
                                            <span class="icon ion-md-cog"></span>
                                            <span>Components</span>
                                            <span class="icon ion-ios-arrow-down"></span>
                                        </div>
                                        <ul class="links collapse" ${ isNormalMode ? 'id="components-links-module-ClusterModule-20fc81e0e913fd0da9a57fdc2994ac39bda50fc1dde5c4c4b73f51b6f87724897be405741f77c3ef21e8ee10e0de19d864ea2fcfed2816e0aca7576eb310d489"' :
                                            'id="xs-components-links-module-ClusterModule-20fc81e0e913fd0da9a57fdc2994ac39bda50fc1dde5c4c4b73f51b6f87724897be405741f77c3ef21e8ee10e0de19d864ea2fcfed2816e0aca7576eb310d489"' }>
                                            <li class="link">
                                                <a href="components/ClusterComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >ClusterComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/ClusterDetailComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >ClusterDetailComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/ClusterListComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >ClusterListComponent</a>
                                            </li>
                                        </ul>
                                    </li>
                                <li class="chapter inner">
                                    <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                        'data-target="#injectables-links-module-ClusterModule-20fc81e0e913fd0da9a57fdc2994ac39bda50fc1dde5c4c4b73f51b6f87724897be405741f77c3ef21e8ee10e0de19d864ea2fcfed2816e0aca7576eb310d489"' : 'data-target="#xs-injectables-links-module-ClusterModule-20fc81e0e913fd0da9a57fdc2994ac39bda50fc1dde5c4c4b73f51b6f87724897be405741f77c3ef21e8ee10e0de19d864ea2fcfed2816e0aca7576eb310d489"' }>
                                        <span class="icon ion-md-arrow-round-down"></span>
                                        <span>Injectables</span>
                                        <span class="icon ion-ios-arrow-down"></span>
                                    </div>
                                    <ul class="links collapse" ${ isNormalMode ? 'id="injectables-links-module-ClusterModule-20fc81e0e913fd0da9a57fdc2994ac39bda50fc1dde5c4c4b73f51b6f87724897be405741f77c3ef21e8ee10e0de19d864ea2fcfed2816e0aca7576eb310d489"' :
                                        'id="xs-injectables-links-module-ClusterModule-20fc81e0e913fd0da9a57fdc2994ac39bda50fc1dde5c4c4b73f51b6f87724897be405741f77c3ef21e8ee10e0de19d864ea2fcfed2816e0aca7576eb310d489"' }>
                                        <li class="link">
                                            <a href="injectables/ClusterService.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >ClusterService</a>
                                        </li>
                                    </ul>
                                </li>
                            </li>
                            <li class="link">
                                <a href="modules/ConfigurationModule.html" data-type="entity-link" >ConfigurationModule</a>
                                    <li class="chapter inner">
                                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                            'data-target="#components-links-module-ConfigurationModule-6a7d32782cda718bbf1c50fb38dafb97f6cc1a7a04257080a990d1b76376989e55d975868c0ff68fa0929ec65748f3e5bc46b388fe3dbd033fae8a54cf0f21a5"' : 'data-target="#xs-components-links-module-ConfigurationModule-6a7d32782cda718bbf1c50fb38dafb97f6cc1a7a04257080a990d1b76376989e55d975868c0ff68fa0929ec65748f3e5bc46b388fe3dbd033fae8a54cf0f21a5"' }>
                                            <span class="icon ion-md-cog"></span>
                                            <span>Components</span>
                                            <span class="icon ion-ios-arrow-down"></span>
                                        </div>
                                        <ul class="links collapse" ${ isNormalMode ? 'id="components-links-module-ConfigurationModule-6a7d32782cda718bbf1c50fb38dafb97f6cc1a7a04257080a990d1b76376989e55d975868c0ff68fa0929ec65748f3e5bc46b388fe3dbd033fae8a54cf0f21a5"' :
                                            'id="xs-components-links-module-ConfigurationModule-6a7d32782cda718bbf1c50fb38dafb97f6cc1a7a04257080a990d1b76376989e55d975868c0ff68fa0929ec65748f3e5bc46b388fe3dbd033fae8a54cf0f21a5"' }>
                                            <li class="link">
                                                <a href="components/ConfigDetailComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >ConfigDetailComponent</a>
                                            </li>
                                        </ul>
                                    </li>
                            </li>
                            <li class="link">
                                <a href="modules/ControllerModule.html" data-type="entity-link" >ControllerModule</a>
                                    <li class="chapter inner">
                                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                            'data-target="#components-links-module-ControllerModule-4a07cbe7498b6c593705baa3f239f542d3f8ef86d23891bcb9c83cb06675e4cbf9126a23481172840989f7d319a7dd97d32b46a81b352856d27c520ef2fd7f71"' : 'data-target="#xs-components-links-module-ControllerModule-4a07cbe7498b6c593705baa3f239f542d3f8ef86d23891bcb9c83cb06675e4cbf9126a23481172840989f7d319a7dd97d32b46a81b352856d27c520ef2fd7f71"' }>
                                            <span class="icon ion-md-cog"></span>
                                            <span>Components</span>
                                            <span class="icon ion-ios-arrow-down"></span>
                                        </div>
                                        <ul class="links collapse" ${ isNormalMode ? 'id="components-links-module-ControllerModule-4a07cbe7498b6c593705baa3f239f542d3f8ef86d23891bcb9c83cb06675e4cbf9126a23481172840989f7d319a7dd97d32b46a81b352856d27c520ef2fd7f71"' :
                                            'id="xs-components-links-module-ControllerModule-4a07cbe7498b6c593705baa3f239f542d3f8ef86d23891bcb9c83cb06675e4cbf9126a23481172840989f7d319a7dd97d32b46a81b352856d27c520ef2fd7f71"' }>
                                            <li class="link">
                                                <a href="components/ControllerDetailComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >ControllerDetailComponent</a>
                                            </li>
                                        </ul>
                                    </li>
                            </li>
                            <li class="link">
                                <a href="modules/CoreModule.html" data-type="entity-link" >CoreModule</a>
                            </li>
                            <li class="link">
                                <a href="modules/DashboardModule.html" data-type="entity-link" >DashboardModule</a>
                                    <li class="chapter inner">
                                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                            'data-target="#components-links-module-DashboardModule-e2ef52cd945b4e54265cb6d5194eb22ecbb6e9dd4e01e8211347d7b9fb8c84d7515a74ffef09e7931ef500f5dd456b3d879c69d892af3db652a144ac0f7d585d"' : 'data-target="#xs-components-links-module-DashboardModule-e2ef52cd945b4e54265cb6d5194eb22ecbb6e9dd4e01e8211347d7b9fb8c84d7515a74ffef09e7931ef500f5dd456b3d879c69d892af3db652a144ac0f7d585d"' }>
                                            <span class="icon ion-md-cog"></span>
                                            <span>Components</span>
                                            <span class="icon ion-ios-arrow-down"></span>
                                        </div>
                                        <ul class="links collapse" ${ isNormalMode ? 'id="components-links-module-DashboardModule-e2ef52cd945b4e54265cb6d5194eb22ecbb6e9dd4e01e8211347d7b9fb8c84d7515a74ffef09e7931ef500f5dd456b3d879c69d892af3db652a144ac0f7d585d"' :
                                            'id="xs-components-links-module-DashboardModule-e2ef52cd945b4e54265cb6d5194eb22ecbb6e9dd4e01e8211347d7b9fb8c84d7515a74ffef09e7931ef500f5dd456b3d879c69d892af3db652a144ac0f7d585d"' }>
                                            <li class="link">
                                                <a href="components/DashboardComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >DashboardComponent</a>
                                            </li>
                                        </ul>
                                    </li>
                            </li>
                            <li class="link">
                                <a href="modules/HistoryModule.html" data-type="entity-link" >HistoryModule</a>
                                    <li class="chapter inner">
                                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                            'data-target="#components-links-module-HistoryModule-7fcd15766edbbe583f7296fbadf420286043f9f992f54c4a6bd459c9a0a390d52da0660c6b21e1ec2c503380e59e6e8e58bfd13b1ed5e8c38f47f403a9cbe9a3"' : 'data-target="#xs-components-links-module-HistoryModule-7fcd15766edbbe583f7296fbadf420286043f9f992f54c4a6bd459c9a0a390d52da0660c6b21e1ec2c503380e59e6e8e58bfd13b1ed5e8c38f47f403a9cbe9a3"' }>
                                            <span class="icon ion-md-cog"></span>
                                            <span>Components</span>
                                            <span class="icon ion-ios-arrow-down"></span>
                                        </div>
                                        <ul class="links collapse" ${ isNormalMode ? 'id="components-links-module-HistoryModule-7fcd15766edbbe583f7296fbadf420286043f9f992f54c4a6bd459c9a0a390d52da0660c6b21e1ec2c503380e59e6e8e58bfd13b1ed5e8c38f47f403a9cbe9a3"' :
                                            'id="xs-components-links-module-HistoryModule-7fcd15766edbbe583f7296fbadf420286043f9f992f54c4a6bd459c9a0a390d52da0660c6b21e1ec2c503380e59e6e8e58bfd13b1ed5e8c38f47f403a9cbe9a3"' }>
                                            <li class="link">
                                                <a href="components/HistoryListComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >HistoryListComponent</a>
                                            </li>
                                        </ul>
                                    </li>
                            </li>
                            <li class="link">
                                <a href="modules/InstanceModule.html" data-type="entity-link" >InstanceModule</a>
                                    <li class="chapter inner">
                                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                            'data-target="#components-links-module-InstanceModule-e64f5c82060cbce58269ff82020d26ae6f22a0aa703dd6e30e34ae17f615964c3419dc7de90bc80753e8d8ae1be707d5d77fe792a85c0c098e8344c8f7e13aa9"' : 'data-target="#xs-components-links-module-InstanceModule-e64f5c82060cbce58269ff82020d26ae6f22a0aa703dd6e30e34ae17f615964c3419dc7de90bc80753e8d8ae1be707d5d77fe792a85c0c098e8344c8f7e13aa9"' }>
                                            <span class="icon ion-md-cog"></span>
                                            <span>Components</span>
                                            <span class="icon ion-ios-arrow-down"></span>
                                        </div>
                                        <ul class="links collapse" ${ isNormalMode ? 'id="components-links-module-InstanceModule-e64f5c82060cbce58269ff82020d26ae6f22a0aa703dd6e30e34ae17f615964c3419dc7de90bc80753e8d8ae1be707d5d77fe792a85c0c098e8344c8f7e13aa9"' :
                                            'id="xs-components-links-module-InstanceModule-e64f5c82060cbce58269ff82020d26ae6f22a0aa703dd6e30e34ae17f615964c3419dc7de90bc80753e8d8ae1be707d5d77fe792a85c0c098e8344c8f7e13aa9"' }>
                                            <li class="link">
                                                <a href="components/InstanceDetailComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >InstanceDetailComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/InstanceListComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >InstanceListComponent</a>
                                            </li>
                                        </ul>
                                    </li>
                                <li class="chapter inner">
                                    <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                        'data-target="#injectables-links-module-InstanceModule-e64f5c82060cbce58269ff82020d26ae6f22a0aa703dd6e30e34ae17f615964c3419dc7de90bc80753e8d8ae1be707d5d77fe792a85c0c098e8344c8f7e13aa9"' : 'data-target="#xs-injectables-links-module-InstanceModule-e64f5c82060cbce58269ff82020d26ae6f22a0aa703dd6e30e34ae17f615964c3419dc7de90bc80753e8d8ae1be707d5d77fe792a85c0c098e8344c8f7e13aa9"' }>
                                        <span class="icon ion-md-arrow-round-down"></span>
                                        <span>Injectables</span>
                                        <span class="icon ion-ios-arrow-down"></span>
                                    </div>
                                    <ul class="links collapse" ${ isNormalMode ? 'id="injectables-links-module-InstanceModule-e64f5c82060cbce58269ff82020d26ae6f22a0aa703dd6e30e34ae17f615964c3419dc7de90bc80753e8d8ae1be707d5d77fe792a85c0c098e8344c8f7e13aa9"' :
                                        'id="xs-injectables-links-module-InstanceModule-e64f5c82060cbce58269ff82020d26ae6f22a0aa703dd6e30e34ae17f615964c3419dc7de90bc80753e8d8ae1be707d5d77fe792a85c0c098e8344c8f7e13aa9"' }>
                                        <li class="link">
                                            <a href="injectables/InstanceService.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >InstanceService</a>
                                        </li>
                                    </ul>
                                </li>
                            </li>
                            <li class="link">
                                <a href="modules/MaterialModule.html" data-type="entity-link" >MaterialModule</a>
                            </li>
                            <li class="link">
                                <a href="modules/ResourceModule.html" data-type="entity-link" >ResourceModule</a>
                                    <li class="chapter inner">
                                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                            'data-target="#components-links-module-ResourceModule-eceaee213440bf988e009aab19d868921a35f5fa5bb2c9df806a47ea6ea094f137bcfc98bac502a02855920bae3dadfeeb48f620c1f1b3f1ddf8be8d5d262a8b"' : 'data-target="#xs-components-links-module-ResourceModule-eceaee213440bf988e009aab19d868921a35f5fa5bb2c9df806a47ea6ea094f137bcfc98bac502a02855920bae3dadfeeb48f620c1f1b3f1ddf8be8d5d262a8b"' }>
                                            <span class="icon ion-md-cog"></span>
                                            <span>Components</span>
                                            <span class="icon ion-ios-arrow-down"></span>
                                        </div>
                                        <ul class="links collapse" ${ isNormalMode ? 'id="components-links-module-ResourceModule-eceaee213440bf988e009aab19d868921a35f5fa5bb2c9df806a47ea6ea094f137bcfc98bac502a02855920bae3dadfeeb48f620c1f1b3f1ddf8be8d5d262a8b"' :
                                            'id="xs-components-links-module-ResourceModule-eceaee213440bf988e009aab19d868921a35f5fa5bb2c9df806a47ea6ea094f137bcfc98bac502a02855920bae3dadfeeb48f620c1f1b3f1ddf8be8d5d262a8b"' }>
                                            <li class="link">
                                                <a href="components/PartitionDetailComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >PartitionDetailComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/PartitionListComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >PartitionListComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/ResourceDetailComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >ResourceDetailComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/ResourceDetailForInstanceComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >ResourceDetailForInstanceComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/ResourceListComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >ResourceListComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/ResourceNodeViewerComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >ResourceNodeViewerComponent</a>
                                            </li>
                                        </ul>
                                    </li>
                                <li class="chapter inner">
                                    <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                        'data-target="#injectables-links-module-ResourceModule-eceaee213440bf988e009aab19d868921a35f5fa5bb2c9df806a47ea6ea094f137bcfc98bac502a02855920bae3dadfeeb48f620c1f1b3f1ddf8be8d5d262a8b"' : 'data-target="#xs-injectables-links-module-ResourceModule-eceaee213440bf988e009aab19d868921a35f5fa5bb2c9df806a47ea6ea094f137bcfc98bac502a02855920bae3dadfeeb48f620c1f1b3f1ddf8be8d5d262a8b"' }>
                                        <span class="icon ion-md-arrow-round-down"></span>
                                        <span>Injectables</span>
                                        <span class="icon ion-ios-arrow-down"></span>
                                    </div>
                                    <ul class="links collapse" ${ isNormalMode ? 'id="injectables-links-module-ResourceModule-eceaee213440bf988e009aab19d868921a35f5fa5bb2c9df806a47ea6ea094f137bcfc98bac502a02855920bae3dadfeeb48f620c1f1b3f1ddf8be8d5d262a8b"' :
                                        'id="xs-injectables-links-module-ResourceModule-eceaee213440bf988e009aab19d868921a35f5fa5bb2c9df806a47ea6ea094f137bcfc98bac502a02855920bae3dadfeeb48f620c1f1b3f1ddf8be8d5d262a8b"' }>
                                        <li class="link">
                                            <a href="injectables/ResourceService.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >ResourceService</a>
                                        </li>
                                    </ul>
                                </li>
                            </li>
                            <li class="link">
                                <a href="modules/SharedModule.html" data-type="entity-link" >SharedModule</a>
                                    <li class="chapter inner">
                                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                            'data-target="#components-links-module-SharedModule-07d08216bfdcb1846358d47d243d79abe1940067b99e4620b47096c8dd704c4ec02f611c1a88e443809e418d41e861c48c8e069e2ad664c1d57c895e303dfdd8"' : 'data-target="#xs-components-links-module-SharedModule-07d08216bfdcb1846358d47d243d79abe1940067b99e4620b47096c8dd704c4ec02f611c1a88e443809e418d41e861c48c8e069e2ad664c1d57c895e303dfdd8"' }>
                                            <span class="icon ion-md-cog"></span>
                                            <span>Components</span>
                                            <span class="icon ion-ios-arrow-down"></span>
                                        </div>
                                        <ul class="links collapse" ${ isNormalMode ? 'id="components-links-module-SharedModule-07d08216bfdcb1846358d47d243d79abe1940067b99e4620b47096c8dd704c4ec02f611c1a88e443809e418d41e861c48c8e069e2ad664c1d57c895e303dfdd8"' :
                                            'id="xs-components-links-module-SharedModule-07d08216bfdcb1846358d47d243d79abe1940067b99e4620b47096c8dd704c4ec02f611c1a88e443809e418d41e861c48c8e069e2ad664c1d57c895e303dfdd8"' }>
                                            <li class="link">
                                                <a href="components/AlertDialogComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >AlertDialogComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/ConfirmDialogComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >ConfirmDialogComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/DataTableComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >DataTableComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/DetailHeaderComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >DetailHeaderComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/DisabledLabelComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >DisabledLabelComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/InputDialogComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >InputDialogComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/InputInlineComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >InputInlineComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/JsonViewerComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >JsonViewerComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/KeyValuePairsComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >KeyValuePairsComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/NodeViewerComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >NodeViewerComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/StateLabelComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >StateLabelComponent</a>
                                            </li>
                                        </ul>
                                    </li>
                                <li class="chapter inner">
                                    <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                        'data-target="#directives-links-module-SharedModule-07d08216bfdcb1846358d47d243d79abe1940067b99e4620b47096c8dd704c4ec02f611c1a88e443809e418d41e861c48c8e069e2ad664c1d57c895e303dfdd8"' : 'data-target="#xs-directives-links-module-SharedModule-07d08216bfdcb1846358d47d243d79abe1940067b99e4620b47096c8dd704c4ec02f611c1a88e443809e418d41e861c48c8e069e2ad664c1d57c895e303dfdd8"' }>
                                        <span class="icon ion-md-code-working"></span>
                                        <span>Directives</span>
                                        <span class="icon ion-ios-arrow-down"></span>
                                    </div>
                                    <ul class="links collapse" ${ isNormalMode ? 'id="directives-links-module-SharedModule-07d08216bfdcb1846358d47d243d79abe1940067b99e4620b47096c8dd704c4ec02f611c1a88e443809e418d41e861c48c8e069e2ad664c1d57c895e303dfdd8"' :
                                        'id="xs-directives-links-module-SharedModule-07d08216bfdcb1846358d47d243d79abe1940067b99e4620b47096c8dd704c4ec02f611c1a88e443809e418d41e861c48c8e069e2ad664c1d57c895e303dfdd8"' }>
                                        <li class="link">
                                            <a href="directives/KeyValuePairDirective.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >KeyValuePairDirective</a>
                                        </li>
                                    </ul>
                                </li>
                                <li class="chapter inner">
                                    <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                        'data-target="#injectables-links-module-SharedModule-07d08216bfdcb1846358d47d243d79abe1940067b99e4620b47096c8dd704c4ec02f611c1a88e443809e418d41e861c48c8e069e2ad664c1d57c895e303dfdd8"' : 'data-target="#xs-injectables-links-module-SharedModule-07d08216bfdcb1846358d47d243d79abe1940067b99e4620b47096c8dd704c4ec02f611c1a88e443809e418d41e861c48c8e069e2ad664c1d57c895e303dfdd8"' }>
                                        <span class="icon ion-md-arrow-round-down"></span>
                                        <span>Injectables</span>
                                        <span class="icon ion-ios-arrow-down"></span>
                                    </div>
                                    <ul class="links collapse" ${ isNormalMode ? 'id="injectables-links-module-SharedModule-07d08216bfdcb1846358d47d243d79abe1940067b99e4620b47096c8dd704c4ec02f611c1a88e443809e418d41e861c48c8e069e2ad664c1d57c895e303dfdd8"' :
                                        'id="xs-injectables-links-module-SharedModule-07d08216bfdcb1846358d47d243d79abe1940067b99e4620b47096c8dd704c4ec02f611c1a88e443809e418d41e861c48c8e069e2ad664c1d57c895e303dfdd8"' }>
                                        <li class="link">
                                            <a href="injectables/HelperService.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >HelperService</a>
                                        </li>
                                    </ul>
                                </li>
                            </li>
                            <li class="link">
                                <a href="modules/TestingModule.html" data-type="entity-link" >TestingModule</a>
                            </li>
                            <li class="link">
                                <a href="modules/WorkflowModule.html" data-type="entity-link" >WorkflowModule</a>
                                    <li class="chapter inner">
                                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                            'data-target="#components-links-module-WorkflowModule-e5885878f7308be21c393d3489b4876e80e02e204744631d7751191f875f9c7d7662b11ef38215101c57ab6cf0918e09f862529c3fb1030abd3b39643a3c7e9f"' : 'data-target="#xs-components-links-module-WorkflowModule-e5885878f7308be21c393d3489b4876e80e02e204744631d7751191f875f9c7d7662b11ef38215101c57ab6cf0918e09f862529c3fb1030abd3b39643a3c7e9f"' }>
                                            <span class="icon ion-md-cog"></span>
                                            <span>Components</span>
                                            <span class="icon ion-ios-arrow-down"></span>
                                        </div>
                                        <ul class="links collapse" ${ isNormalMode ? 'id="components-links-module-WorkflowModule-e5885878f7308be21c393d3489b4876e80e02e204744631d7751191f875f9c7d7662b11ef38215101c57ab6cf0918e09f862529c3fb1030abd3b39643a3c7e9f"' :
                                            'id="xs-components-links-module-WorkflowModule-e5885878f7308be21c393d3489b4876e80e02e204744631d7751191f875f9c7d7662b11ef38215101c57ab6cf0918e09f862529c3fb1030abd3b39643a3c7e9f"' }>
                                            <li class="link">
                                                <a href="components/JobDetailComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >JobDetailComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/JobListComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >JobListComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/WorkflowDagComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >WorkflowDagComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/WorkflowDetailComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >WorkflowDetailComponent</a>
                                            </li>
                                            <li class="link">
                                                <a href="components/WorkflowListComponent.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >WorkflowListComponent</a>
                                            </li>
                                        </ul>
                                    </li>
                                <li class="chapter inner">
                                    <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ?
                                        'data-target="#injectables-links-module-WorkflowModule-e5885878f7308be21c393d3489b4876e80e02e204744631d7751191f875f9c7d7662b11ef38215101c57ab6cf0918e09f862529c3fb1030abd3b39643a3c7e9f"' : 'data-target="#xs-injectables-links-module-WorkflowModule-e5885878f7308be21c393d3489b4876e80e02e204744631d7751191f875f9c7d7662b11ef38215101c57ab6cf0918e09f862529c3fb1030abd3b39643a3c7e9f"' }>
                                        <span class="icon ion-md-arrow-round-down"></span>
                                        <span>Injectables</span>
                                        <span class="icon ion-ios-arrow-down"></span>
                                    </div>
                                    <ul class="links collapse" ${ isNormalMode ? 'id="injectables-links-module-WorkflowModule-e5885878f7308be21c393d3489b4876e80e02e204744631d7751191f875f9c7d7662b11ef38215101c57ab6cf0918e09f862529c3fb1030abd3b39643a3c7e9f"' :
                                        'id="xs-injectables-links-module-WorkflowModule-e5885878f7308be21c393d3489b4876e80e02e204744631d7751191f875f9c7d7662b11ef38215101c57ab6cf0918e09f862529c3fb1030abd3b39643a3c7e9f"' }>
                                        <li class="link">
                                            <a href="injectables/JobService.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >JobService</a>
                                        </li>
                                        <li class="link">
                                            <a href="injectables/WorkflowService.html" data-type="entity-link" data-context="sub-entity" data-context-id="modules" >WorkflowService</a>
                                        </li>
                                    </ul>
                                </li>
                            </li>
                </ul>
                </li>
                    <li class="chapter">
                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ? 'data-target="#classes-links"' :
                            'data-target="#xs-classes-links"' }>
                            <span class="icon ion-ios-paper"></span>
                            <span>Classes</span>
                            <span class="icon ion-ios-arrow-down"></span>
                        </div>
                        <ul class="links collapse " ${ isNormalMode ? 'id="classes-links"' : 'id="xs-classes-links"' }>
                            <li class="link">
                                <a href="classes/Cluster.html" data-type="entity-link" >Cluster</a>
                            </li>
                            <li class="link">
                                <a href="classes/Controller.html" data-type="entity-link" >Controller</a>
                            </li>
                            <li class="link">
                                <a href="classes/HelixCtrl.html" data-type="entity-link" >HelixCtrl</a>
                            </li>
                            <li class="link">
                                <a href="classes/History.html" data-type="entity-link" >History</a>
                            </li>
                            <li class="link">
                                <a href="classes/Instance.html" data-type="entity-link" >Instance</a>
                            </li>
                            <li class="link">
                                <a href="classes/Job.html" data-type="entity-link" >Job</a>
                            </li>
                            <li class="link">
                                <a href="classes/Node.html" data-type="entity-link" >Node</a>
                            </li>
                            <li class="link">
                                <a href="classes/Partition.html" data-type="entity-link" >Partition</a>
                            </li>
                            <li class="link">
                                <a href="classes/Resource.html" data-type="entity-link" >Resource</a>
                            </li>
                            <li class="link">
                                <a href="classes/Settings.html" data-type="entity-link" >Settings</a>
                            </li>
                            <li class="link">
                                <a href="classes/Task.html" data-type="entity-link" >Task</a>
                            </li>
                            <li class="link">
                                <a href="classes/UserCtrl.html" data-type="entity-link" >UserCtrl</a>
                            </li>
                            <li class="link">
                                <a href="classes/Workflow.html" data-type="entity-link" >Workflow</a>
                            </li>
                        </ul>
                    </li>
                        <li class="chapter">
                            <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ? 'data-target="#injectables-links"' :
                                'data-target="#xs-injectables-links"' }>
                                <span class="icon ion-md-arrow-round-down"></span>
                                <span>Injectables</span>
                                <span class="icon ion-ios-arrow-down"></span>
                            </div>
                            <ul class="links collapse " ${ isNormalMode ? 'id="injectables-links"' : 'id="xs-injectables-links"' }>
                                <li class="link">
                                    <a href="injectables/ConfigurationService.html" data-type="entity-link" >ConfigurationService</a>
                                </li>
                                <li class="link">
                                    <a href="injectables/ControllerService.html" data-type="entity-link" >ControllerService</a>
                                </li>
                                <li class="link">
                                    <a href="injectables/HelixService.html" data-type="entity-link" >HelixService</a>
                                </li>
                                <li class="link">
                                    <a href="injectables/HistoryService.html" data-type="entity-link" >HistoryService</a>
                                </li>
                                <li class="link">
                                    <a href="injectables/UserService.html" data-type="entity-link" >UserService</a>
                                </li>
                            </ul>
                        </li>
                    <li class="chapter">
                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ? 'data-target="#guards-links"' :
                            'data-target="#xs-guards-links"' }>
                            <span class="icon ion-ios-lock"></span>
                            <span>Guards</span>
                            <span class="icon ion-ios-arrow-down"></span>
                        </div>
                        <ul class="links collapse " ${ isNormalMode ? 'id="guards-links"' : 'id="xs-guards-links"' }>
                            <li class="link">
                                <a href="guards/ClusterResolver.html" data-type="entity-link" >ClusterResolver</a>
                            </li>
                            <li class="link">
                                <a href="guards/ResourceResolver.html" data-type="entity-link" >ResourceResolver</a>
                            </li>
                        </ul>
                    </li>
                    <li class="chapter">
                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ? 'data-target="#interfaces-links"' :
                            'data-target="#xs-interfaces-links"' }>
                            <span class="icon ion-md-information-circle-outline"></span>
                            <span>Interfaces</span>
                            <span class="icon ion-ios-arrow-down"></span>
                        </div>
                        <ul class="links collapse " ${ isNormalMode ? ' id="interfaces-links"' : 'id="xs-interfaces-links"' }>
                            <li class="link">
                                <a href="interfaces/HelixSession.html" data-type="entity-link" >HelixSession</a>
                            </li>
                            <li class="link">
                                <a href="interfaces/HelixUserRequest.html" data-type="entity-link" >HelixUserRequest</a>
                            </li>
                            <li class="link">
                                <a href="interfaces/IReplica.html" data-type="entity-link" >IReplica</a>
                            </li>
                            <li class="link">
                                <a href="interfaces/ListFieldObject.html" data-type="entity-link" >ListFieldObject</a>
                            </li>
                            <li class="link">
                                <a href="interfaces/MapFieldObject.html" data-type="entity-link" >MapFieldObject</a>
                            </li>
                            <li class="link">
                                <a href="interfaces/SimpleFieldObject.html" data-type="entity-link" >SimpleFieldObject</a>
                            </li>
                        </ul>
                    </li>
                    <li class="chapter">
                        <div class="simple menu-toggler" data-toggle="collapse" ${ isNormalMode ? 'data-target="#miscellaneous-links"'
                            : 'data-target="#xs-miscellaneous-links"' }>
                            <span class="icon ion-ios-cube"></span>
                            <span>Miscellaneous</span>
                            <span class="icon ion-ios-arrow-down"></span>
                        </div>
                        <ul class="links collapse " ${ isNormalMode ? 'id="miscellaneous-links"' : 'id="xs-miscellaneous-links"' }>
                            <li class="link">
                                <a href="miscellaneous/functions.html" data-type="entity-link">Functions</a>
                            </li>
                            <li class="link">
                                <a href="miscellaneous/typealiases.html" data-type="entity-link">Type aliases</a>
                            </li>
                            <li class="link">
                                <a href="miscellaneous/variables.html" data-type="entity-link">Variables</a>
                            </li>
                        </ul>
                    </li>
                    <li class="chapter">
                        <a data-type="chapter-link" href="coverage.html"><span class="icon ion-ios-stats"></span>Documentation coverage</a>
                    </li>
                    <li class="divider"></li>
                    <li class="copyright">
                        Documentation generated using <a href="https://compodoc.app/" target="_blank">
                            <img data-src="images/compodoc-vectorise.png" class="img-responsive" data-type="compodoc-logo">
                        </a>
                    </li>
            </ul>
        </nav>
        `);
        this.innerHTML = tp.strings;
    }
});